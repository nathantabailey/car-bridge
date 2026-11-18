#!/usr/local/venv/bin/python3
"""
Comma 3X Car Bridge Daemon - car_bridge.py
Version: 1.1.0 (Fixed)
Location: /data/car_bridge.py (persists across OTA)

Responsibilities:
- SubMaster msgq consumer (carState @ 10 Hz from 100 Hz publisher)
- WebSocket :8080 for DashPilot display (shared-secret token)
- HTTP :8081 JSON snapshot endpoint
- CSV logging: 10 Hz state, append+flush, daily rotation, 7 days
- Event logging: thresholded events only
- Drive summary: start/end logic with speed thresholds

Security:
- Shared-secret token required on WebSocket connections
- Binds to specific interface (not 0.0.0.0)
"""

import asyncio
import json
import os
import secrets
import shutil
import signal
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List

import aiohttp.web as web
import cereal.messaging as messaging


# =============================================================================
# CONFIGURATION
# =============================================================================

SCRIPT_PATH = Path(__file__)
DATA_DIR = Path("/data")
LOG_DIR = DATA_DIR / "log"
CSV_DIR = LOG_DIR / "csv"
EVENTS_FILE = LOG_DIR / "events.log"
SUMMARIES_FILE = LOG_DIR / "summaries.json"
SECRET_FILE = DATA_DIR / "car_bridge_secret.txt"

ROTATION_DAYS = 7
ROTATION_INTERVAL_HOURS = 1
SAMPLE_RATE_HZ = 10

DRIVE_START_THRESHOLD_MS = 0.5   # m/s
DRIVE_END_THRESHOLD_MS = 0.1     # m/s
HARD_BRAKE_ACCEL_MS2 = -3.0      # m/s²
HARD_ACCEL_ACCEL_MS2 = 3.0       # m/s²

BIND_ADDRESS = os.environ.get("CAR_BRIDGE_BIND", "10.142.x.x")
WEBSOCKET_PORT = 8080
HTTP_PORT = 8081

ENABLE_GPS = True

CSV_COLUMNS = [
    'timestamp', 'vEgo_ms', 'aEgo_ms2', 'vEgoCluster_ms', 'standstill',
    'gear', 'leftBlinker', 'rightBlinker', 'doorsOpen', 'seatbeltFastened',
    'cruiseState', 'gps_lat', 'gps_lon', 'gps_age_ms',
]


# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging():
    """Configure file-based logging."""
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        formatter = logging.Formatter(
            '%(asctime)s [%(name)s] %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        handler = logging.FileHandler(LOG_DIR / 'car_bridge.log')
        handler.setFormatter(formatter)
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        root_logger = logging.getLogger('car_bridge')
        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(handler)
        root_logger.addHandler(console_handler)
        
    except Exception as e:
        print(f"[FATAL] Failed to setup logging: {e}")


# =============================================================================
# STATE CLASSES
# =============================================================================

class DriveState:
    def __init__(self):
        self.started = False
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.duration_seconds: float = 0.0
        self.max_speed_ms: float = 0.0


class EventState:
    def __init__(self):
        self.hard_brakes = 0
        self.hard_accels = 0


# =============================================================================
# MAIN DAEMON
# =============================================================================

class BridgeDaemon:
    def __init__(self):
        self.running = False
        self.shutdown_event = asyncio.Event()
        self._shutting_down = False
        
        self.sub_master: Optional[messaging.SubMaster] = None
        self.gps_sub: Optional[messaging.SubMaster] = None
        
        self.drive = DriveState()
        self.events = EventState()
        
        self._start_counter = 0
        self._end_counter = 0
        
        self.csv_write_handle = None
        self.current_csv_file: Optional[Path] = None
        self.last_rotation: Optional[datetime] = None
        
        self.secret_token: str = ""
        
        self.web_server: Optional[web.Application] = None
        self.runner: Optional[web.AppRunner] = None
        self.ws_clients: List[web.WebSocketResponse] = []
        
        self.heartbeat_task: Optional[asyncio.Task] = None
        self.log = logging.getLogger('car_bridge')

    async def init(self):
        """Initialize daemon, load config."""
        setup_logging()
        log = self.log
        log.info("[init] Starting car_bridge.py on %s", SCRIPT_PATH)
        
        CSV_DIR.mkdir(parents=True, exist_ok=True)
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        
        await self._load_or_generate_secret()
        
        self.sub_master = messaging.SubMaster(['carState'], log_all=False)
        if ENABLE_GPS:
            self.gps_sub = messaging.SubMaster(['gpsLocationExternal'], log_all=False)
        else:
            self.gps_sub = None
        
        self.heartbeat_task = asyncio.create_task(self._ws_heartbeat_loop())
        
        log.info("[init] SubMaster initialized")

    async def _load_or_generate_secret(self):
        """Load or generate shared secret."""
        if SECRET_FILE.exists():
            with open(SECRET_FILE, 'r') as f:
                self.secret_token = f.read().strip()
        else:
            self.secret_token = secrets.token_hex(32)
            with open(SECRET_FILE, 'w') as f:
                f.write(self.secret_token)
            SECRET_FILE.chmod(0o600)
        
        log.info("[init] Secret loaded/generated (len=%d)", len(self.secret_token))

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

    async def _rotate_csv(self):
        """Rotate CSV files older than 7 days."""
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=ROTATION_DAYS)
        
        for csv_file in CSV_DIR.glob("car_*.csv"):
            try:
                if csv_file.stat().st_mtime < cutoff.timestamp():
                    log.info("[rotate] Deleting old CSV: %s", csv_file.name)
                    csv_file.unlink()
            except Exception as e:
                log.error("[rotate] Error deleting %s: %s", csv_file, e)

    def _write_drive_summary(self):
        """Atomically write drive summary to JSON file."""
        if not self.drive.started or self.drive.end_time is None:
            return
        
        summary = {
            'start_time': self.drive.start_time.isoformat(),
            'end_time': self.drive.end_time.isoformat(),
            'duration_seconds': round(self.drive.duration_seconds, 2),
            'max_speed_ms': round(self.drive.max_speed_ms, 3),
            'max_speed_kmh': round(self.drive.max_speed_ms * 3.6, 2),
            'hard_brakes': self.events.hard_brakes,
            'hard_accels': self.events.hard_accels,
        }
        
        temp_path = SUMMARIES_FILE.with_suffix('.tmp')
        try:
            with open(temp_path, 'w') as f:
                json.dump(summary, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            
            shutil.move(str(temp_path), str(SUMMARIES_FILE))
            log.info("[summary] Wrote drive summary to %s", SUMMARIES_FILE)
        except Exception as e:
            log.error("[summary] Failed to write summary: %s", e)

    def _log_event(self, event_type: str, data: Dict[str, Any]):
        """Append discrete event to events.log."""
        try:
            with open(EVENTS_FILE, 'a') as f:
                f.write(json.dumps(data) + "\n")
                f.flush()
        except Exception as e:
            self.log.error("[log_event] Failed to write: %s", e)

    async def _write_csv_row(self, cs, gps_data: Optional[Any] = None):
        """Append CSV row atomically."""
        line_data = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'vEgo_ms': float(cs.vEgo),
            'aEgo_ms2': float(cs.aEgo),
            'vEgoCluster_ms': float(cs.vEgoCluster),
            'standstill': bool(cs.standstill),
            'gear': str(cs.gear),
            'leftBlinker': cs.leftBlinkers,
            'rightBlinker': cs.rightBlinker,
            'doorsOpen': cs.doorOpen,
            'seatbeltFastened': not cs.seatbeltUnbuckled,
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
        
        csv_line = ','.join(str(line_data.get(col, '')) for col in CSV_COLUMNS) + "\n"
        
        if self.csv_write_handle is None:
            filename = f"car_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
            self.current_csv_file = CSV_DIR / filename
            self.csv_write_handle = open(self.current_csv_file, 'a', buffering=1)
            self.csv_write_handle.write(','.join(CSV_COLUMNS) + "\n")
            log.info("[csv] Created: %s", self.current_csv_file.name)
        
        self.csv_write_handle.write(csv_line)
        self.csv_write_handle.flush()

    def _process_car_state(self, cs):
        """Process carState message."""
        try:
            vEgo_ms = float(cs.vEgo)
            aEgo_ms2 = float(cs.aEgo)
            
            if not self.drive.started and vEgo_ms > DRIVE_START_THRESHOLD_MS:
                self._start_counter += 1
                if self._start_counter >= 30:
                    self.drive.started = True
                    self.drive.start_time = datetime.now(timezone.utc)
                    log.info("[drive] Started at %s", self.drive.start_time.isoformat())
            
            if self.drive.started:
                self.drive.max_speed_ms = max(self.drive.max_speed_ms, vEgo_ms)
                
                if vEgo_ms < DRIVE_END_THRESHOLD_MS:
                    self._end_counter += 1
                    if self._end_counter >= 100:
                        self.drive.end_time = datetime.now(timezone.utc)
                        self.drive.duration_seconds = (self.drive.end_time - self.drive.start_time).total_seconds()
                        log.info("[drive] Ended at %s, duration=%.1fs", 
                                self.drive.end_time.isoformat(), 
                                self.drive.duration_seconds)
                        
                        self._write_drive_summary()
                
                if aEgo_ms2 < HARD_BRAKE_ACCEL_MS2:
                    self.events.hard_brakes += 1
                    self._log_event('hard_brake', {
                        'event': 'hard_brake',
                        'aEgo_ms2': round(aEgo_ms2, 3),
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    })
                
                if aEgo_ms2 > HARD_ACCEL_ACCEL_MS2:
                    self.events.hard_accels += 1
                    self._log_event('hard_accel', {
                        'event': 'hard_accel',
                        'aEgo_ms2': round(aEgo_ms2, 3),
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    })
        
        except Exception as e:
            log.error("[process] Error processing carState: %s", e)

    def _get_latest_json_snapshot(self) -> Dict[str, Any]:
        """Return latest JSON snapshot."""
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'vEgo_ms': float(self.sub_master['carState'].vEgo),
            'aEgo_ms2': float(self.sub_master['carState'].aEgo),
            'driveStarted': self.drive.started,
            'hardBrakes': self.events.hard_brakes,
            'hardAccels': self.events.hard_accels,
        }

    async def _ws_handler(self, request: web.Request):
        """WebSocket handler for DashPilot."""
        token = request.query.get('token')
        if not token or token != self.secret_token:
            return web.Response(status=403, text="Forbidden")
        
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        self.ws_clients.append(ws)
        log.info("[ws] Client connected. Total: %d", len(self.ws_clients))
        
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.ERROR:
                    break
        except Exception as e:
            log.error("[ws] Error: %s", e)
        finally:
            if ws in self.ws_clients:
                self.ws_clients.remove(ws)
                log.info("[ws] Client disconnected. Total: %d", len(self.ws_clients))
        
        return ws

    async def _http_handler(self, request: web.Request):
        """HTTP JSON snapshot endpoint."""
        secret = request.headers.get('X-Car-Bridge-Secret')
        if not secret or secret != self.secret_token:
            return web.Response(status=403, text="Forbidden")
        
        data = self._get_latest_json_snapshot()
        return web.json_response(data)

    async def run(self):
        """Main daemon loop."""
        log.info("[run] Starting main loop")
        self.running = True
        
        app = web.Application()
        app.router.add_get('/', self._http_handler)
        app.router.add_get('/ws', self._ws_handler)
        
        async def on_startup(app):
            self.runner = web.AppRunner(app)
            await self.runner.setup()
            
            http_site = web.TCPSite(self.runner, BIND_ADDRESS, HTTP_PORT)
            await http_site.start()
            log.info("[http] Server listening on %s:%d", BIND_ADDRESS, HTTP_PORT)
        
        app.on_startup.append(on_startup)
        
        last_rotation = datetime.now(timezone.utc)
        
        try:
            async with self.sub_master:
                if ENABLE_GPS:
                    async with self.gps_sub:
                        while not self.shutdown_event.is_set():
                            self.sub_master.update()
                            
                            if self.sub_master.updated.get('carState'):
                                cs = self.sub_master['carState']
                                self._process_car_state(cs)
                                
                                gps_data = None
                                if self.gps_sub.updated.get('gpsLocationExternal'):
                                    gps_data = self.gps_sub['gpsLocationExternal']
                                
                                await self._write_csv_row(cs, gps_data)
                            
                            # Broadcast to WebSocket clients
                            if self.ws_clients:
                                snapshot = json.dumps(self._get_latest_json_snapshot())
                                for ws in self.ws_clients[:]:
                                    try:
                                        await ws.send_str(snapshot)
                                    except Exception:
                                        if ws in self.ws_clients:
                                            self.ws_clients.remove(ws)
                            
                            # Periodic rotation check
                            if (datetime.now(timezone.utc) - last_rotation).total_seconds() >= ROTATION_INTERVAL_HOURS * 3600:
                                await self._rotate_csv()
                                last_rotation = datetime.now(timezone.utc)
                                
                                if self.csv_write_handle and not self.csv_write_handle.closed:
                                    self.csv_write_handle.close()
                                    self.csv_write_handle = None
                            
                            await asyncio.sleep(1.0 / SAMPLE_RATE_HZ)
                
        except Exception as e:
            log.error("[run] Fatal error: %s", e)
        finally:
            await self.shutdown()

    async def shutdown(self):
        """Graceful shutdown."""
        if getattr(self, '_shutting_down', False):
            return
        self._shutting_down = True
        
        log.info("[shutdown] Shutting down...")
        
        self.running = False
        self.shutdown_event.set()
        
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
        
        if self.csv_write_handle and not self.csv_write_handle.closed:
            self.csv_write_handle.close()
        
        if self.runner:
            await self.runner.cleanup()
        
        log.info("[shutdown] Complete")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

async def main():
    daemon = BridgeDaemon()
    
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(daemon.shutdown()))
    
    try:
        await daemon.init()
        await daemon.run()
    except Exception as e:
        print(f"[FATAL] {e}")


if __name__ == "__main__":
    asyncio.run(main())
