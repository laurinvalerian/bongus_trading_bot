import subprocess
import time
import sys
import threading
from urllib import request, parse
import os
from dotenv import load_dotenv

# Load env to get Telegram API credentials for direct messaging
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN_BONGUS")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID_BONGUS")

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
    }
}

MAX_CRASHES_BEFORE_SOS = 3
CRASH_WINDOW_SECONDS = 60

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

def monitor_loop():
    print("👑 King Watchdog initialized and watching over the kingdom.\n")
    
    # Initial startup of all services
    for name in PROCESSES.keys():
        spawn_process(name)
        time.sleep(2) # Give each a moment to breathe before launching the next

    try:
        while True:
            time.sleep(5) # Wake up every 5 seconds to check the children
            current_time = time.time()

            for name, p_info in PROCESSES.items():
                proc = p_info["process"]
                
                # Check if process is dead
                if proc.poll() is not None:
                    exit_code = proc.returncode
                    
                    # If it crashed recently, increment the rapid-crash counter
                    if current_time - p_info["last_crash_time"] < CRASH_WINDOW_SECONDS:
                        p_info["crash_count"] += 1
                    else:
                        p_info["crash_count"] = 1 # Reset if it was a long time ago
                        
                    p_info["last_crash_time"] = current_time
                    
                    print(f"⚠️ King Watchdog: App '{name}' died with code {exit_code}! (Crash #{p_info['crash_count']})")
                    
                    # Too many crashes? Shut it down and call an ambulance
                    if p_info["crash_count"] >= MAX_CRASHES_BEFORE_SOS:
                        warning_msg = f"‼️ FATAL ALERT‼️\nThe process '{name}' has crashed {MAX_CRASHES_BEFORE_SOS} times within {CRASH_WINDOW_SECONDS} seconds.\n\nKing Watchdog has suspended restarts for this module. Manual intervention required!"
                        send_sos_telegram_alert(warning_msg)
                        
                        # Stop tracking it so we don't spam telegram
                        p_info["crash_count"] = -999 
                        continue
                        
                    # Otherwise, restart it
                    if p_info["crash_count"] > 0:
                        send_sos_telegram_alert(f"⚠️ App '{name}' crashed (Code {exit_code}). King Watchdog is restarting it...")
                        spawn_process(name)

    except KeyboardInterrupt:
        print("\n👑 King Watchdog shutting down... Killing all child domains.")
        for name, p_info in PROCESSES.items():
            if p_info["process"]:
                p_info["process"].terminate()
        sys.exit(0)

if __name__ == "__main__":
    monitor_loop()