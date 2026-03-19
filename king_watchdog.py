import subprocess
import time
import sys
from urllib import request, parse
import os
from dotenv import load_dotenv

# Load env to get Telegram API credentials for direct messaging
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN_BONGUS")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID_BONGUS")

OPTIMIZER_ENABLED = os.getenv("OPTIMIZER_ENABLED", "true").lower() == "true"
OPTIMIZER_OPTIONAL = os.getenv("OPTIMIZER_OPTIONAL", "true").lower() == "true"
OPTIMIZER_INTERVAL_HOURS = int(os.getenv("OPTIMIZER_INTERVAL_HOURS", "12"))
OPTIMIZER_INTERVAL_SECONDS = max(1, OPTIMIZER_INTERVAL_HOURS) * 3600
MAX_OPTIMIZER_FAILURES_BEFORE_DISABLE = 3
RUST_ENGINE_WARMUP_SECONDS = int(os.getenv("RUST_ENGINE_WARMUP_SECONDS", "20"))

# Master configuration for the microservices
PROCESSES = {
    "Rust_Engine": {
        "command": ["cargo", "run", "--release"],
        "cwd": "execution_engine", # Run from the rust folder
        "process": None,
        "crash_count": 0,
        "last_crash_time": 0
    },
    "Live_Trader": {
        "command": [sys.executable, "live_trader.py"],
        "cwd": ".",
        "process": None,
        "crash_count": 0,
        "last_crash_time": 0
    },
    "Dashboard": {
        "command": [sys.executable, "web_dashboard.py"],
        "cwd": ".",
        "process": None,
        "crash_count": 0,
        "last_crash_time": 0
    },
    "Telegram_Alerter": {
        "command": [sys.executable, "telegram_alerter.py"],
        "cwd": ".",
        "process": None,
        "crash_count": 0,
        "last_crash_time": 0
    },
    "Continual_Optimizer": {
        "command": [sys.executable, "continual_optimizer.py"],
        "cwd": ".",
        "process": None,
        "crash_count": 0,
        "last_crash_time": 0,
        "scheduled": True,
    }
}

CORE_PROCESS_NAMES = ["Rust_Engine", "Live_Trader", "Dashboard", "Telegram_Alerter"]

MAX_CRASHES_BEFORE_SOS = 3
CRASH_WINDOW_SECONDS = 60


class SchedulerState:
    def __init__(self):
        self.disabled = not OPTIMIZER_ENABLED
        self.next_optimizer_run_ts = time.time() + OPTIMIZER_INTERVAL_SECONDS
        self.optimizer_failures = 0
        self.last_handled_optimizer_pid = None

def send_sos_telegram_alert(message: str):
    """Sends a critical alert without relying on the external alerter script"""
    print(f"🚨 SENT SOS TELEGRAM: {message}")
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram credentials missing from .env!")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = parse.urlencode({'chat_id': TELEGRAM_CHAT_ID, 'text': message}).encode('utf-8')
    try:
        req = request.Request(url, data=data)
        request.urlopen(req)
    except Exception as e:
        print(f"Failed to send SOS telegram: {e}")

def spawn_process(name):
    """Starts or restarts a specific microservice"""
    p_info = PROCESSES[name]
    print(f"👑 King Watchdog: Starting {name}...")
    
    # We launch them without taking over standard out so they can log naturally.
    p_info["process"] = subprocess.Popen(
        p_info["command"],
        cwd=p_info["cwd"]
    )


def handle_core_process_crash(name, p_info, current_time):
    proc = p_info["process"]
    if proc is None or proc.poll() is None:
        return

    exit_code = proc.returncode

    if current_time - p_info["last_crash_time"] < CRASH_WINDOW_SECONDS:
        p_info["crash_count"] += 1
    else:
        p_info["crash_count"] = 1

    p_info["last_crash_time"] = current_time
    print(f"⚠️ King Watchdog: App '{name}' died with code {exit_code}! (Crash #{p_info['crash_count']})")

    if p_info["crash_count"] >= MAX_CRASHES_BEFORE_SOS:
        warning_msg = (
            f"‼️ FATAL ALERT‼️\nThe process '{name}' has crashed {MAX_CRASHES_BEFORE_SOS} "
            f"times within {CRASH_WINDOW_SECONDS} seconds.\n\n"
            "King Watchdog has suspended restarts for this module. Manual intervention required!"
        )
        send_sos_telegram_alert(warning_msg)
        p_info["crash_count"] = -999
        return

    send_sos_telegram_alert(f"⚠️ App '{name}' crashed (Code {exit_code}). King Watchdog is restarting it...")
    spawn_process(name)


def handle_optimizer_schedule(state: SchedulerState):
    if state.disabled:
        return

    now = time.time()
    optimizer_info = PROCESSES["Continual_Optimizer"]
    optimizer_proc = optimizer_info["process"]

    if now >= state.next_optimizer_run_ts and (optimizer_proc is None or optimizer_proc.poll() is not None):
        print("🔄 King Watchdog: Launching scheduled Continual_Optimizer run...")
        spawn_process("Continual_Optimizer")
        state.next_optimizer_run_ts = now + OPTIMIZER_INTERVAL_SECONDS

    optimizer_proc = optimizer_info["process"]
    if optimizer_proc is None or optimizer_proc.poll() is None:
        return

    if state.last_handled_optimizer_pid == optimizer_proc.pid:
        return

    state.last_handled_optimizer_pid = optimizer_proc.pid
    exit_code = optimizer_proc.returncode
    optimizer_info["process"] = None

    if exit_code == 0:
        state.optimizer_failures = 0
        print("✅ Continual_Optimizer completed successfully.")
        return

    state.optimizer_failures += 1
    msg = (
        f"⚠️ Continual_Optimizer failed with code {exit_code} "
        f"(Failure #{state.optimizer_failures}/{MAX_OPTIMIZER_FAILURES_BEFORE_DISABLE})."
    )
    print(msg)
    send_sos_telegram_alert(msg)

    if state.optimizer_failures >= MAX_OPTIMIZER_FAILURES_BEFORE_DISABLE:
        state.disabled = True
        send_sos_telegram_alert("‼️ Continual_Optimizer scheduler disabled after repeated failures.")
        if not OPTIMIZER_OPTIONAL:
            raise RuntimeError("Continual_Optimizer failed repeatedly and OPTIMIZER_OPTIONAL is false.")

def monitor_loop():
    print("👑 King Watchdog initialized and watching over the kingdom.\n")
    scheduler_state = SchedulerState()

    if OPTIMIZER_ENABLED:
        print(f"⏰ Optimizer scheduling enabled: every {OPTIMIZER_INTERVAL_HOURS}h (optional={OPTIMIZER_OPTIONAL})")
    else:
        print("⏸️ Optimizer scheduling disabled by OPTIMIZER_ENABLED=false")
    
    # Initial startup of all services
    spawn_process("Rust_Engine")
    print(f"⏳ Waiting {RUST_ENGINE_WARMUP_SECONDS}s for Rust engine warm-up before starting live trader...")
    time.sleep(RUST_ENGINE_WARMUP_SECONDS)
    for name in ["Live_Trader", "Dashboard", "Telegram_Alerter"]:
        spawn_process(name)
        time.sleep(2) # Give each a moment to breathe before launching the next

    try:
        while True:
            time.sleep(5) # Wake up every 5 seconds to check the children
            current_time = time.time()

            for name in CORE_PROCESS_NAMES:
                handle_core_process_crash(name, PROCESSES[name], current_time)

            handle_optimizer_schedule(scheduler_state)

    except KeyboardInterrupt:
        print("\n👑 King Watchdog shutting down... Killing all child domains.")
        for name, p_info in PROCESSES.items():
            if p_info["process"]:
                p_info["process"].terminate()
        sys.exit(0)
    except Exception as e:
        print(f"\n👑 King Watchdog fatal error: {e}. Shutting down children.")
        for name, p_info in PROCESSES.items():
            if p_info["process"]:
                p_info["process"].terminate()
        sys.exit(1)

if __name__ == "__main__":
    monitor_loop()