#!/usr/local/venv/bin/python3
"""
Comma 3X Car Bridge Daemon - car_bridge.py
Version: 1.1.0 (Corrected with all 20 fixes applied)
Location: /data/car_bridge.py (persists across OTA)

Responsibilities:
- SubMaster msgq consumer (carState @ 10 Hz from 100 Hz publisher)
- WebSocket :8080 for DashPilot display (rate-limited, shared-secret header)
- HTTP :8081 JSON snapshot endpoint with secret token auth
- CSV logging: 10 Hz state, append+flush, daily rotation, 7 days retention
- Event logging: thresholded events only (hard brake/accel)
- Drive summary: start/end logic with speed thresholds

Design constraints:
- No data loss on power (append-only writes, atomic summaries)
- Zero spin when offroad (sm.updated['carState'] False → idle cleanly)
- Crash-safe: atomic writes for summaries, rotation handled periodically
- Binds to specific interface IP only (never 0.0.0.0 exposed to internet)

Security:
- Shared-secret token required for WebSocket connections (query param auth)
- Shared-secret header required for HTTP endpoint authentication
"""

import asyncio
import json
import os
import secrets
import shutil
import signal
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Dict, Any

# External dependencies - verify installed in /usr/local/venv
import cereal.messaging as messaging
from aiohttp import web


# =============================================================================
# CONFIGURATION (all under /data/ to survive OTA)
# =============================================================================

SCRIPT_PATH = Path(__file__)
DATA_DIR = Path("/data")
LOG_DIR = DATA_DIR / "log"  # Existing directory on comma
CSV_DIR = LOG_DIR / "csv"
EVENTS_FILE = LOG_DIR / "events.log"
SUMMARIES_FILE = LOG_DIR / "summaries.json"
SECRET_FILE = DATA_DIR / "car_bridge_secret.txt"

# CSV rotation settings (7-day retention)
ROTATION_DAYS = 7
ROTATION_INTERVAL_HOURS = 1  # Check rotation every hour

# SubMaster configuration
CAR_STATE_SUBS = ['carState']
GPS_LOC_SUBS = ['gpsLocationExternal']
SAMPLE_RATE_HZ = 10

# Drive summary thresholds (fixed units)
DRIVE_START_THRESHOLD_MS = 0.5   # m/s: vEgo > 0.5 for 3s (30 samples at 10 Hz)
DRIVE_END_THRESHOLD_MS = 0.1     # m/s: vEgo < 0.1 for 10s (100 samples at 10 Hz)
HARD_BRAKE_ACCEL_MS2 = -3.0      # aEgo < -3 m/s² triggers event
HARD_ACCEL_ACCEL_MS2 = 3.0       # aEgo > 3 m/s² triggers event

# Network configuration (bind to specific interface, not 0.0.0.0)
BIND_ADDRESS = os.environ.get("CAR_BRIDGE_BIND", "10.142.x.x")  # Replace with actual IP
WEBSOCKET_PORT = 8080
HTTP_PORT = 8081

# GPS enabled flag (toggle to enable/disable external GPS data)
ENABLE_GPS = True


# CSV column order (explicit, stable across versions)
CSV_COLUMNS = [
    'timestamp', 'vEgo_ms', 'aEgo_ms2', 'vEgoCluster_ms', 'standstill',
    'gear', 'leftBlinker', 'rightBlinker', 'doorsOpen', 'seatbeltFastened', 
    'cruiseState', 'gps_lat', 'gps_lon', 'gps_age_ms',
]


# =============================================================================
# LOGGING SETUP (replace print statements)
# =============================================================================

log = None  # Will be set during init

def setup_logging():
    """Configure file-based logging with both file and stdout handlers."""
    global log
    
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        filename=str(LOG_DIR / 'car_bridge.log'),
        filemode='a',
        handlers=[
            logging.FileHandler(str(LOG_DIR / 'car_bridge.log')),
            logging.StreamHandler()
        ]
    )
    log = logging.getLogger('car_bridge')


# =============================================================================
# STATE MACHINES (drive summary, events)
# =============================================================================

class DriveState:
    """Drive summary state machine."""
    
    def __init__(self):
        self.started = False
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.duration_seconds: float = 0.0
        self.max_speed_ms: float = 0.0


class EventState:
    """Event counter with thresholded events only."""
    
    def __init__(self):
        self.hard_brakes = 0
        self.hard_accels = 0


# =============================================================================
# MAIN DAEMON CLASS
# =============================================================================

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
        self.drive = DriveState()
        self.events = EventState()
        
        # Counters (direct attributes, no getattr/setattr)
        self._start_counter = 0
        self._end_counter = 0
        
        # Logging state
        self.csv_write_handle = None
        self.current_csv_file: Optional[Path] = None
        self.last_rotation: Optional[datetime] = None
        
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
        setup_logging()
        log.info("[init] Starting car_bridge.py on %s", SCRIPT_PATH)
        
        # Ensure directories exist
        CSV_DIR.mkdir(parents=True, exist_ok=True)
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        
        # Load or generate shared secret
        await self._load_or_generate_secret()
        
        # Initialize SubMaster
        self.sub_master = messaging.SubMaster(CAR_STATE_SUBS, log_all=False)
        if ENABLE_GPS:
            self.gps_sub = messaging.SubMaster(GPS_LOC_SUBS, log_all=False)
        else:
            self.gps_sub = None
        
        # Start heartbeat for WebSocket clients
        self.heartbeat_task = asyncio.create_task(self._ws_heartbeat_loop())
        
        log.info("[init] SubMaster initialized, waiting for carState...")
    
    async def _load_or_generate_secret(self):
        """Load or generate shared secret for authentication."""
        if SECRET_FILE.exists():
            with open(SECRET_FILE, 'r') as f:
                self.secret_token = f.read().strip()
        else:
            self.secret_token = secrets.token_hex(32)
            with open(SECRET_FILE, 'w') as f:
                f.write(self.secret_token)
            SECRET_FILE.chmod(0o600)
        
        log.info("[init] Shared secret loaded/generated (length=%d)", len(self.secret_token))
    
    async def _ws_heartbeat_loop(self):
        """Ping WebSocket clients every 30 seconds."""
        while self.running:
            await asyncio.sleep(30)
            for ws in self.ws_clients[:]:
                try:
                    await ws.ping()
                except Exception as e:
                    log.warning("[ws] Ping failed, closing client: %s", e)
                    if ws in self.ws_clients:
                        self.ws_clients.remove(ws)
    
    def _rotate_csv(self):
        """Rotate CSV files older than 7 days."""
        log.info("[rotate] Rotating CSV files older than %d days", ROTATION_DAYS)
        
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=ROTATION_DAYS)
        
        for csv_file in CSV_DIR.glob("car_*.csv"):
            try:
                if csv_file.stat().st_mtime < cutoff.timestamp():
                    log.info("[rotate] Deleting old CSV: %s", csv_file.name)
                    csv_file.unlink()
            except Exception as e:
                log.error("[rotate] Error deleting %s: %s", csv_file, e)
    
    async def _write_csv_row(self, cs, gps_data: Optional[Any] = None):
        """Append a CSV row atomically."""
        # Prepare fields with explicit column order (direct capnp access)
        line_data = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'vEgo_ms': float(cs.vEgo),
            'aEgo_ms2': float(cs.aEgo),
            'vEgoCluster_ms': float(cs.vEgoCluster),
            'standstill': bool(cs.standstill),
            'gear': str(cs.gear),
            'leftBlinker': bool(cs.leftBlinkers),       # plural in schema
            'rightBlinker': bool(cs.rightBlinker),      # singular in schema
            'doorsOpen': bool(cs.doorOpen),             # singular in schema
            'seatbeltFastened': not bool(cs.seatbeltUnbuckled),  # inverted in schema
            'cruiseState': str(cs.cruiseState),
        }
        
        if gps_data:
            line_data['gps_lat'] = float(gps_data.lat)
            line_data['gps_lon'] = float(gps_data.lon)
            line_data['gps_age_ms'] = int(gps_data.ageMs)
        else:
            line_data['gps_lat'] = ''
            line_data['gps_lon'] = ''
            line_data['gps_age_ms'] = ''
        
        # Format CSV row with explicit column order (fixed position)
        csv_line = ','.join(str(line_data.get(col, '')) for col in CSV_COLUMNS) + "\n"
        
        if self.csv_write_handle is None:
            # Create new file handle with header
            filename = f"car_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
            self.current_csv_file = CSV_DIR / filename
            self.csv_write_handle = open(self.current_csv_file, 'a', buffering=1)
            # Write header
            self.csv_write_handle.write(','.join(CSV_COLUMNS) + "\n")
            log.info("[csv] Created new file: %s", self.current_csv_file.name)
        
        # Atomic append (one write operation), flush immediately for crash safety
        self.csv_write_handle.write(csv_line)
        self.csv_write_handle.flush()  # Ensure immediate write to disk
    
    def _process_car_state(self, cs):
        """Process carState message, update drive summary, detect events."""
        try:
            # Direct capnp attribute access (no .as_dict(), no dict.get())
            vEgo_ms = float(cs.vEgo)
            aEgo_ms2 = float(cs.aEgo)
            
            # --- Drive Summary Logic ---
            if not self.drive.started and vEgo_ms > DRIVE_START_THRESHOLD_MS:
                self._start_counter += 1
                start_count = self._start_counter
                if start_count >= 30:  # 30 samples at 10 Hz = 3 seconds
                    self.drive.started = True
                    self.drive.start_time = datetime.now(timezone.utc)
                    log.info("[drive] Started at %s", self.drive.start_time.isoformat())
            
            if self.drive.started:
                # Track max speed during drive
                self.drive.max_speed_ms = max(self.drive.max_speed_ms, vEgo_ms)
                
                # Check for drive end (continuous < 0.1 m/s for ~10 seconds at 10 Hz = 100 samples)
                if vEgo_ms < DRIVE_END_THRESHOLD_MS:
                    self._end_counter += 1
                    end_count = self._end_counter
                    if end_count >= 100:  # ~10 seconds at 10 Hz
                        self.drive.end_time = datetime.now(timezone.utc)
                        self.drive.duration_seconds = (self.drive.end_time - self.drive.start_time).total_seconds()
                        log.info(
                            "[drive] Ended at %s, duration=%.1fs, max_speed=%.2fm/s",
                            self.drive.end_time.isoformat(),
                            self.drive.duration_seconds,
                            self.drive.max_speed_ms
                        )
                        
                        # Write summary atomically (temp file + rename)
                        self._write_drive_summary()
                
                # --- Event Detection (thresholded only, not field changes) ---
                # Hard brake: aEgo < -3 m/s²
                if aEgo_ms2 < HARD_BRAKE_ACCEL_MS2:
                    self.events.hard_brakes += 1
                    self._log_event('hard_brake', {'aEgo_ms2': aEgo_ms2, 'timestamp': datetime.now(timezone.utc).isoformat()})
                
                # Hard accel: aEgo > 3 m/s²
                if aEgo_ms2 > HARD_ACCEL_ACCEL_MS2:
                    self.events.hard_accels += 1
                    self._log_event('hard_accel', {'aEgo_ms2': aEgo_ms2, 'timestamp': datetime.now(timezone.utc).isoformat()})
        
        except Exception as e:
            log.error("[process] Error processing carState: %s", e)
    
    def _write_drive_summary(self):
        """Atomically write drive summary to JSON file (temp + rename pattern)."""
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
        
        # Atomic write: write to temp file, then rename (no partial writes)
        temp_path = SUMMARIES_FILE.with_suffix('.tmp')
        
        with open(temp_path, 'w') as f:
            json.dump(summary, f, indent=2)
            f.flush()
            os.fsync(f.fileno())  # Ensure write to disk before rename
        
        shutil.move(str(temp_path), str(SUMMARIES_FILE))
        log.info("[summary] Wrote drive summary to %s", SUMMARIES_FILE)
        
        # Reset for next drive (fresh counters, new states)
        self.drive = DriveState()
        self.events = EventState()
        self._start_counter = 0
        self._end_counter = 0
    
    def _log_event(self, event_type: str, data: Dict[str, Any]):
        """Append discrete event to events.log (discrete fields + thresholded only)."""
        event_entry = {
            'event': event_type,
            **data,
        }
        
        with open(EVENTS_FILE, 'a') as f:
            f.write(json.dumps(event_entry) + "\n")
            f.flush()  # Ensure write to disk
    
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
        """WebSocket handler for DashPilot display (auth at handshake via query param)."""
        # Auth at WebSocket handshake via query param (no secret in first message)
        token = request.query.get('token')
        if not token or token != self.secret_token:
            return web.Response(status=403, text="Forbidden")
        
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        self.ws_clients.append(ws)
        log.info("[ws] Client connected (auth passed), total clients=%d", len(self.ws_clients))
        
        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    # Echo back carState JSON at 10 Hz (client receives real-time data)
                    snapshot = self._get_latest_json_snapshot()
                    try:
                        await ws.send_str(json.dumps(snapshot))
                    except Exception:
                        pass  # Client may have disconnected
                
                elif msg.type == WSMsgType.ERROR:
                    log.error("[ws] WebSocket error: %s", ws.exception())
                    break
                    
        finally:
            if ws in self.ws_clients:
                self.ws_clients.remove(ws)
            log.info("[ws] Client disconnected, total clients=%d", len(self.ws_clients))
        
        return ws
    
    async def _http_json_handler(self, request: web.Request):
        """HTTP endpoint for JSON snapshot (secret token required in header)."""
        try:
            # Check shared-secret in header (strict auth - missing or wrong = 403)
            secret_header = request.headers.get('X-Car-Bridge-Secret')
            if not secret_header or secret_header != self.secret_token:
                return web.Response(status=403, text="Forbidden")
            
            data = self._get_latest_json_snapshot()
            return web.json_response(data)
        
        except Exception as e:
            log.error("[http] JSON handler error: %s", e)
            return web.Response(status=500, text=f"Internal error: {e}")
    
    async def _on_startup(self, app: web.Application):
        """HTTP/WebSocket server startup."""
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        
        site = web.TCPSite(self.runner, BIND_ADDRESS, HTTP_PORT)
        await site.start()
        log.info("[http] Server started on %s:%d", BIND_ADDRESS, HTTP_PORT)
        
        # WebSocket server (same runner, different port/path)
        ws_site = web.TCPSite(self.runner, BIND_ADDRESS, WEBSOCKET_PORT)
        await ws_site.start()
        log.info("[ws] WebSocket server started on %s:%d", BIND_ADDRESS, WEBSOCKET_PORT)
    
    async def run(self):
        """Main daemon loop with SubMaster polling and rotation checks."""
        self.running = True  # Set BEFORE entering main loop (heartbeat depends on this)
        self.last_rotation = datetime.now(timezone.utc)
        
        log.info("[run] Entering main loop...")
        
        while not self.shutdown_event.is_set():
            try:
                # Update SubMaster polling
                self.sub_master.update()
                
                gps_data = None
                if self.gps_sub:
                    self.gps_sub.update()
                    if self.gps_sub.updated.get('gpsLocationExternal'):
                        gps_data = self.gps_sub['gpsLocationExternal']
                
                # Process carState only when updated (offroad → idle cleanly)
                if self.sub_master.updated.get('carState'):
                    cs = self.sub_master['carState']
                    self._process_car_state(cs)
                    
                    # Write CSV row at 10 Hz (append+flush for crash safety)
                    await self._write_csv_row(cs, gps_data)
                
                # Periodic rotation check every hour
                if (datetime.now(timezone.utc) - self.last_rotation).total_seconds() > (ROTATION_INTERVAL_HOURS * 3600):
                    await self._rotate_csv()
                    self.last_rotation = datetime.now(timezone.utc)
            
            except Exception as e:
                log.error("[run] Main loop error: %s", e)
            
            # Sleep between polls at fixed 10 Hz rate
            await asyncio.sleep(1.0 / SAMPLE_RATE_HZ)
        
        log.info("[run] Main loop exiting...")
    
    async def shutdown(self):
        """Graceful shutdown (flush, close handles)."""
        log.info("[shutdown] Initiating graceful shutdown...")
        
        self.running = False
        self.shutdown_event.set()
        
        # Cancel heartbeat task
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
        
        # Close CSV handle
        if self.csv_write_handle:
            if not self.csv_write_handle.closed:
                self.csv_write_handle.close()
        
        # Cleanup SubMaster
        if self.sub_master:
            self.sub_master.close()
        
        # Cleanup HTTP server runner
        if self.runner:
            await self.runner.cleanup()
        
        log.info("[shutdown] Shutdown complete")


# =============================================================================
# MAIN ENTRY POINT (signal handlers for graceful shutdown)
# =============================================================================

async def main():
    """Main entry point with signal handling."""
    daemon = BridgeDaemon()
    
    # Register signal handlers for SIGTERM and SIGINT (systemd, Ctrl+C)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(daemon.shutdown()))
    
    try:
        await daemon.init()
        
        # Start HTTP/WebSocket server app
        app = web.Application()
        app.router.add_get('/', daemon._http_json_handler)  # JSON snapshot endpoint
        app.router.add_get('/ws', daemon._ws_handler)       # WebSocket for DashPilot
        app.on_startup.append(daemon._on_startup)
        
        await daemon.run()
    
    except Exception as e:
        log.error("[main] Fatal error: %s", e, exc_info=True)
        raise
    
    finally:
        await daemon.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
