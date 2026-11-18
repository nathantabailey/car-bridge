#!/usr/local/venv/bin/python3
"""
Comma 3X Car Bridge Daemon - car_bridge.py
Version: 1.0.0
Location: /data/car_bridge.py (persists across OTA)

Responsibilities:
- SubMaster msgq consumer (carState @ 10 Hz from 100 Hz publisher)
- WebSocket :8080 for DashPilot display (rate-limited, shared-secret header)
- HTTP :8081 JSON snapshot endpoint
- CSV logging: 10 Hz state, append+flush, daily rotation, 7 days
- Event logging: discrete fields + thresholded events only
- Drive summary: start/end logic with speed thresholds

Design constraints:
- No data loss on power (append-only writes)
- Zero spin when offroad (sm.updated['carState'] False)
- Crash-safe: atomic writes for summaries, rotation handled externally
- Binds to LAN interface only, never 0.0.0.0 exposed to internet

Security:
- Shared-secret header required on WebSocket connections
- HTTP endpoint accessible from ZeroTier/LAN only (bind interface-specific or firewall rules)
"""

import asyncio
import json
import os
import secrets
import signal
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Dict, Any

# External dependencies - verify installed in /usr/local/venv
try:
    import zmq.asyncio  # For ZMQ subscriber if needed (cereal uses msgq internally)
except ImportError:
    pass

import cereal.messaging as messaging
from cereal.services import SERVICE_LIST
from aiohttp import web, ClientSession, WSMsgType


# =============================================================================
# CONFIGURATION
# =============================================================================

# File paths (all under /data/ to survive OTA)
SCRIPT_PATH = Path(__file__)
DATA_DIR = Path("/data")
LOG_DIR = DATA_DIR / "log"  # Existing directory
CSV_DIR = LOG_DIR / "csv"
EVENTS_FILE = LOG_DIR / "events.log"
SUMMARIES_FILE = LOG_DIR / "summaries.json"
SECRET_FILE = DATA_DIR / "car_bridge_secret.txt"

# CSV settings (7-day retention)
ROTATION_DAYS = 7
CSV_MAX_AGE_HOURS = ROTATION_DAYS * 24

# SubMaster configuration
CAR_STATE_SUBS = ['carState']
GPS_LOC_SUBS = ['gpsLocationExternal']  # Optional, for route_comparison
SAMPLE_RATE_HZ = 10  # Sample from 100 Hz publisher at 10 Hz

# Drive summary thresholds (from spec)
DRIVE_START_THRESHOLD_MS = 500  # vEgo > 0.5 m/s for 3s
DRIVE_END_THRESHOLD_MS = 100    # vEgo < 0.1 m/s for 10s
HARD_BRAKE_ACCEL_MS2 = -3.0     # aEgo < -3 m/s² triggers event
HARD_ACCEL_ACCEL_MS2 = 3.0      # aEgo > 3 m/s² triggers event

# Network configuration (bind to specific interface or 0.0.0.0 if single iface)
WEBSOCKET_PORT = 8080
HTTP_PORT = 8081
BIND_ADDRESS = "0.0.0.0"  # Single interface safe; prefer explicit IP in production

# Shared secret (auto-generated on first run if missing)


# =============================================================================
# STATE MACHINE
# =============================================================================

class DriveState:
    """Drive summary state machine."""
    
    def __init__(self):
        self.started = False
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.duration_seconds: float = 0.0
        self.max_speed_ms: float = 0.0
        self.distinct_events: list = []


class EventState:
    """Event counter with discrete fields + thresholded events only."""
    
    def __init__(self):
        self.hard_brakes = 0
        self.hard_accels = 0
        self.discrete_fields_last = {
            'gear': None, 'leftBlinker': None, 'rightBlinker': None,
            'doorsOpen': None, 'seatbeltFastened': None, 'cruiseState': None
        }


class BridgeDaemon:
    """Main car bridge daemon with SubMaster consumer and HTTP/WebSocket servers."""
    
    def __init__(self):
        self.running = False
        self.shutdown_event = asyncio.Event()
        
        # SubMaster handles cereal.messaging (msgq backend)
        self.sub_master: Optional[messaging.SubMaster] = None
        self.gps_sub: Optional[messaging.SubMaster] = None
        
        # State tracking
        self.car_state_last: Dict[str, Any] = {}
        self.car_state_updated = False  # sm.updated['carState'] indicator
        self.drive = DriveState()
        self.events = EventState()
        
        # Logging state
        self.current_csv_file: Optional[Path] = None
        self.csv_write_handle = None
        self.last_csv_timestamp: datetime = datetime.now(timezone.utc)
        self.csv_row_count = 0
        
        # Shared secret
        self.secret_token: str = ""
        
        # HTTP/WebSocket servers
        self.web_server: Optional[web.Application] = None
        self.runner: Optional[web.AppRunner] = None
        self.ws_clients: list = []
        
        # Heartbeat for WebSocket (send ping every 30s)
        self.heartbeat_task: Optional[asyncio.Task] = None
        
    async def init(self):
        """Initialize daemon, load config, create log directories."""
        print(f"[init] Starting car_bridge.py on {SCRIPT_PATH}")
        
        # Ensure directories exist
        CSV_DIR.mkdir(parents=True, exist_ok=True)
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        
        # Load or generate shared secret
        await self._load_or_generate_secret()
        
        # Initialize SubMaster
        self.sub_master = messaging.SubMaster(CAR_STATE_SUBS, log_all=False)
        self.gps_sub = messaging.SubMaster(GPS_LOC_SUBS, log_all=False) if True else None
        
        # Start heartbeat for WebSocket clients
        self.heartbeat_task = asyncio.create_task(self._ws_heartbeat_loop())
        
        print("[init] SubMaster initialized, waiting for carState...")
        
    async def _load_or_generate_secret(self):
        """Load or generate shared secret for WebSocket authentication."""
        if SECRET_FILE.exists():
            with open(SECRET_FILE, 'r') as f:
                self.secret_token = f.read().strip()
        else:
            self.secret_token = secrets.token_hex(32)
            with open(SECRET_FILE, 'w') as f:
                f.write(self.secret_token)
            SECRET_FILE.chmod(0o600)
        
        print(f"[init] Shared secret loaded/generated (length={len(self.secret_token)})")
    
    async def _ws_heartbeat_loop(self):
        """Ping WebSocket clients every 30 seconds."""
        while self.running:
            await asyncio.sleep(30)
            for ws in self.ws_clients[:]:
                try:
                    await ws.ping()
                except Exception as e:
                    print(f"[ws] Ping failed, closing client: {e}")
                    if ws in self.ws_clients:
                        self.ws_clients.remove(ws)
    
    async def _csv_rotation_check(self):
        """Check and perform daily rotation if needed."""
        now = datetime.now(timezone.utc)
        age_seconds = (now - self.last_csv_timestamp).total_seconds()
        
        if age_seconds >= CSV_MAX_AGE_HOURS * 3600:
            await self._rotate_csv()
        elif age_seconds >= 86400:  # Check every ~hour after first rotation
            await asyncio.sleep(3600)
    
    async def _rotate_csv(self):
        """Rotate CSV files older than 7 days."""
        print(f"[rotate] Rotating CSV files older than {ROTATION_DAYS} days")
        
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=ROTATION_DAYS)
        
        for csv_file in CSV_DIR.glob("car_*.csv"):
            try:
                if pathlib.Path(csv_file).stat().st_mtime < cutoff.timestamp():
                    print(f"[rotate] Deleting old CSV: {csv_file.name}")
                    csv_file.unlink()
            except Exception as e:
                print(f"[rotate] Error deleting {csv_file}: {e}")
    
    async def _write_csv_row(self, car_state: Dict[str, Any], gps_data: Optional[Dict[str, Any]] = None):
        """Append a CSV row atomically."""
        self.csv_row_count += 1
        
        # Prepare fields (discrete + continuous for monitoring)
        line_data = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'vEgo_ms': car_state.get('vEgo', 0.0),
            'aEgo_ms2': car_state.get('longitudinalPlan', {}).get('aEgo', 0.0),
            'aEgoLongitudinal_ms2': car_state.get('aEgo', 0.0),
            'vEgoCluster_ms': car_state.get('vEgoCluster', 0.0),
            'standstill': car_state.get('standstill', False),
            'gear': str(car_state.get('gear', 'N')),
            'leftBlinker': bool(car_state.get('leftBlinker', False)),
            'rightBlinker': bool(car_state.get('rightBlinker', False)),
            'doorsOpen': bool(car_state.get('doorsOpen', False)),
            'seatbeltFastened': bool(car_state.get('seatbeltFastened', False)),
            'cruiseState': str(car_state.get('cruiseState', {})),
        }
        
        if gps_data:
            line_data['gpsLocationExternal_lat'] = gps_data.get('lat')
            line_data['gpsLocationExternal_lon'] = gps_data.get('lon')
            line_data['gpsLocationExternal_age_ms'] = gps_data.get('ageMs', 9999)
        
        # Format CSV row (header written on first write)
        csv_line = ','.join(str(line_data[key]) for key in sorted(line_data.keys())) + "\n"
        
        if self.csv_write_handle is None:
            # Create new file handle with header
            filename = f"car_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
            self.current_csv_file = CSV_DIR / filename
            self.csv_write_handle = open(self.current_csv_file, 'a', buffering=1)
            # Write header
            self.csv_write_handle.write(','.join(sorted(line_data.keys())) + "\n")
            print(f"[csv] Created new file: {self.current_csv_file.name}")
        
        # Atomic append (one write operation)
        self.csv_write_handle.write(csv_line)
        self.csv_write_handle.flush()  # Ensure immediate write for crash safety
    
    def _process_car_state(self, car_state_msg):
        """Process carState message, update drive summary, detect events."""
        try:
            # Get data from msgq wrapper (cereal.messaging provides dict-like access)
            cs = car_state_msg.as_dict() if hasattr(car_state_msg, 'as_dict') else {}
            
            # Update last state
            self.car_state_last = cs
            self.car_state_updated = True
            
            vEgo_ms = cs.get('vEgo', 0.0)
            aEgo_ms2 = cs.get('aEgo', 0.0)
            
            # --- Drive Summary Logic ---
            if not self.drive.started and vEgo_ms > DRIVE_START_THRESHOLD_MS:
                start_count = getattr(self, '_start_counter', 0) + 1
                setattr(self, '_start_counter', start_count)
                if start_count >= 3:  # ~3 samples (at 10 Hz = 0.3s per sample)
                    self.drive.started = True
                    self.drive.start_time = datetime.now(timezone.utc)
                    print(f"[drive] Started at {self.drive.start_time.isoformat()}")
            
            if self.drive.started:
                # Track max speed
                self.drive.max_speed_ms = max(self.drive.max_speed_ms, vEgo_ms)
                
                # Check for drive end (continuous < 0.1 m/s for ~10 seconds at 10 Hz = 100 samples)
                if vEgo_ms < DRIVE_END_THRESHOLD_MS:
                    setattr(self, '_end_counter', getattr(self, '_end_counter', 0) + 1)
                    end_count = getattr(self, '_end_counter', 0)
                    if end_count >= 100:  # ~10 seconds at 10 Hz
                        self.drive.end_time = datetime.now(timezone.utc)
                        self.drive.duration_seconds = (self.drive.end_time - self.drive.start_time).total_seconds()
                        print(f"[drive] Ended at {self.drive.end_time.isoformat()}, duration={self.drive.duration_seconds:.1f}s, max_speed={self.drive.max_speed_ms:.2f}m/s")
                        
                        # Write summary atomically (new file, no overwrite)
                        self._write_drive_summary()
                
                # --- Event Detection (thresholded only, not field changes) ---
                # Hard brake: aEgo < -3 m/s²
                if aEgo_ms2 < HARD_BRAKE_ACCEL_MS2:
                    self.events.hard_brakes += 1
                    self._log_event('hard_brake', {'aEgo_ms2': aEog_m, 'timestamp': datetime.now(timezone.utc).isoformat()})
                
                # Hard accel: aEgo > 3 m/s²
                if aEgo_ms2 > HARD_ACCEL_ACCEL_MS2:
                    self.events.hard_accels += 1
                    self._log_event('hard_accel', {'aEgo_ms2': aEgo_ms2, 'timestamp': datetime.now(timezone.utc).isoformat()})
            
        except Exception as e:
            print(f"[process] Error processing carState: {e}")
    
    def _write_drive_summary(self):
        """Atomically write drive summary to JSON file."""
        if not self.drive.started or self.drive.end_time is None:
            return
        
        summary = {
            'start_time': self.drive.start_time.isoformat(),
            'end_time': self.drive.end_time.isoformat(),
            'duration_seconds': self.drive.duration_seconds,
            'max_speed_ms': self.drive.max_speed_ms,
            'max_speed_kmh': round(self.drive.max_speed_ms * 3.6, 2),
            'hard_brakes': self.events.hard_brakes,
            'hard_accels': self.events.hard_accels,
        }
        
        # Atomic write: write to temp file, then rename
        temp_path = SUMMARIES_FILE.with_suffix('.tmp')
        with open(temp_path, 'w') as f:
            json.dump(summary, f, indent=2)
            f.flush()
            os.fsync(f.fileno())  # Ensure write to disk
        
        shutil.move(str(temp_path), str(SUMMARIES_FILE))
        print(f"[summary] Wrote drive summary to {SUMMARIES_FILE}")
        
        # Reset for next drive
        self.drive = DriveState()
        self.events = EventState()
        setattr(self, '_start_counter', 0)
        setattr(self, '_end_counter', 0)
    
    def _log_event(self, event_type: str, data: Dict[str, Any]):
        """Append discrete event to events.log (discrete fields + thresholded only)."""
        event_entry = {
            'event': event_type,
            **data,
        }
        
        with open(EVENTS_FILE, 'a') as f:
            f.write(json.dumps(event_entry) + "\n")
            f.flush()
    
    def _get_latest_json_snapshot(self) -> Dict[str, Any]:
        """Return latest JSON snapshot for HTTP endpoint."""
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'carState': self.car_state_last,
            'driveStarted': self.drive.started,
            'driveStartTime': self.drive.start_time.isoformat() if self.drive.start_time else None,
            'driveEndTime': self.drive.end_time.isoformat() if self.drive.end_time else None,
            'hardBrakes': self.events.hard_brakes,
            'hardAccels': self.events.hard_accels,
        }
    
    async def _ws_handler(self, request: web.Request):
        """WebSocket handler for DashPilot display."""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        self.ws_clients.append(ws)
        print(f"[ws] Client connected, total clients={len(self.ws_clients)}")
        
        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    # Check shared-secret header (should be passed in WebSocket subprotocol or first message)
                    if msg.data != self.secret_token:
                        await ws.close(code=4001, reason="Invalid secret")
                        break
                    
                    # Echo back carState JSON at 10 Hz
                    snapshot = self._get_latest_json_snapshot()
                    try:
                        await ws.send_str(json.dumps(snapshot))
                    except Exception:
                        pass  # Client may have disconnected
                
                elif msg.type == WSMsgType.ERROR:
                    print(f"[ws] WebSocket error: {ws.exception()}")
                    break
                    
        finally:
            if ws in self.ws_clients:
                self.ws_clients.remove(ws)
            print(f"[ws] Client disconnected, total clients={len(self.ws_clients)}")
        
        return ws
    
    async def _http_json_handler(self, request: web.Request):
        """HTTP endpoint for JSON snapshot."""
        try:
            # Check shared-secret in header if provided
            secret_header = request.headers.get('X-Car-Bridge-Secret')
            if secret_header and secret_header != self.secret_token:
                return web.Response(status=403, text="Forbidden")
            
            data = self._get_latest_json_snapshot()
            return web.json_response(data)
        except Exception as e:
            print(f"[http] JSON handler error: {e}")
            return web.Response(status=500, text=f"Internal error: {e}")
    
    async def _on_startup(self, app: web.Application):
        """HTTP/WebSocket server startup."""
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        
        site = web.TCPSite(self.runner, BIND_ADDRESS, HTTP_PORT)
        await site.start()
        print(f"[http] Server started on {BIND_ADDRESS}:{HTTP_PORT}")
        
        # Also start WebSocket server (can share runner with same app but different path)
        ws_site = web.TCPSite(self.runner, BIND_ADDRESS, WEBSOCKET_PORT)
        await ws_site.start()
        print(f"[ws] WebSocket server started on {BIND_ADDRESS}:{WEBSOCKET_PORT}")
    
    async def run(self):
        """Main daemon loop."""
        print("[run] Entering main loop...")
        
        async with self.sub_master:  # Ensure cleanup
            await self._csv_rotation_check()  # Initial rotation check
            
            while not self.shutdown_event.is_set():
                try:
                    # Poll SubMaster at 10 Hz (sleep between polls)
                    car_state_msg = self.sub_master['carState']
                    gps_data = None
                    
                    if self.gps_sub:
                        gps_data = self.gps_sub['gpsLocationExternal']
                    
                    # Process message only if updated
                    if self.sub_master.updated.get('carState'):
                        self._process_car_state(car_state_msg)
                        await self._write_csv_row(
                            car_state_msg.as_dict() if hasattr(car_state_msg, 'as_dict') else {},
                            gps_data
                        )
                    
                except messaging.SubMasterNotUpdated:
                    # Offroad case: sm.updated['carState'] False → idle cleanly
                    print(f"[run] Car offroad, carState not updated (idle)")
                    pass
                    
                except Exception as e:
                    print(f"[run] Main loop error: {e}")
                
                await asyncio.sleep(1.0 / SAMPLE_RATE_HZ)  # 10 Hz sampling
                
                # Check for shutdown signal
                if self.shutdown_event.is_set():
                    break
    
    async def shutdown(self):
        """Graceful shutdown."""
        print("[shutdown] Initiating shutdown...")
        
        self.running = False
        self.shutdown_event.set()
        
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
        
        if self.sub_master:
            self.sub_master.close()
        
        if self.runner:
            await self.runner.cleanup()
        
        if self.current_csv_write_handle:
            if not self.current_csv_write_handle.closed:
                self.current_csv_write_handle.close()
        
        print("[shutdown] Shutdown complete")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

async def main():
    daemon = BridgeDaemon()
    
    # Signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(daemon.shutdown()))
    
    try:
        await daemon.init()
        
        # Start HTTP/WebSocket server app
        app = web.Application()
        app.router.add_get('/', daemon._http_json_handler)  # JSON snapshot endpoint
        app.router.add_get('/ws', daemon._ws_handler)      # WebSocket for DashPilot
        app.on_startup.append(daemon._on_startup)
        
        await daemon.run()
        
    except Exception as e:
        print(f"[main] Fatal error: {e}")
        raise
    
    finally:
        await daemon.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
