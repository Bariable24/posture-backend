"""
Real-Time Posture Detection — Backend
--------------------------------------
Role in the architecture:
  Raspberry Pi (MPU6050) --POST every X sec--> THIS SERVICE (Render) --Socket.IO push--> Vercel frontend (simulation + control pages)

Why the Pi doesn't talk to the frontend directly:
  - Vercel serves static/serverless pages with no persistent socket endpoint of its own.
  - Centralizing the posture-classification LOGIC here means the Pi only ever does two things:
    read the sensor, and POST a small JSON payload. All the math and state (calibration,
    thresholds, history, alerts) lives in one place and can be updated without touching
    the device firmware.
  - This mirrors the ESP32 pattern the project report uses; nothing here is
    Pi-specific, so it also works unmodified if you swap back to an ESP32 later.

Endpoints:
  POST /api/sensor      <- Pi pushes a raw reading here
  GET  /api/latest       -> most recent processed reading (REST fallback / polling)
  GET  /api/history       -> last N processed readings
  POST /api/calibrate    <- frontend asks to set current pose as "neutral"
  GET  /api/commands      -> Pi polls this to see if the frontend queued anything (e.g. recalibrate, buzzer test)
  POST /api/commands/ack  <- Pi acknowledges a command so it isn't sent again
  POST /api/config        <- frontend updates sensitivity thresholds
  GET  /api/config        -> current thresholds
  WS   /socket.io         <- 'posture_update' events pushed to the frontend in real time

Run locally:
  pip install -r requirements.txt
  python app.py
"""

import math
import time
from collections import deque
from threading import Lock

from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO

app = Flask(__name__)
# In production, replace "*" with your actual Vercel domain(s) for tighter security,
# e.g. CORS(app, origins=["https://your-project.vercel.app"])
CORS(app, resources={r"/api/*": {"origins": "*"}})
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# ---------------------------------------------------------------------------
# State (in-memory — fine for a single-device MVP; swap for a DB/Redis if you
# add multiple sensors or need persistence across restarts)
# ---------------------------------------------------------------------------
lock = Lock()

HISTORY_MAXLEN = 500
history = deque(maxlen=HISTORY_MAXLEN)
latest_reading = None

# Neutral / "good posture" baseline captured during calibration.
baseline = {"pitch": 0.0, "roll": 0.0, "calibrated": False}

# Sensitivity thresholds (degrees of deviation from baseline before we flag a state).
# Exposed via /api/config so the control page's sliders can tune these live.
config = {
    "forward_head_threshold": 12.0,   # pitch deviation forward -> slouch/forward head
    "kyphosis_threshold": 20.0,       # larger forward pitch sustained -> rounded upper back
    "swayback_threshold": -10.0,      # pitch deviation backward -> swayback / leaning back
    "lateral_threshold": 10.0,        # roll deviation either direction -> leaning left/right
    "sustained_alert_seconds": 15.0,  # how long a bad posture must persist before alerting
}

# Pending commands queued by the control page for the Pi to pick up on its next poll.
# e.g. {"id": "...", "type": "recalibrate"} or {"type": "buzzer_test"}
command_queue = []

# Tracks how long the current posture state has been sustained, for alerting.
state_tracker = {"state": "unknown", "since": time.time()}


# ---------------------------------------------------------------------------
# Posture math
# ---------------------------------------------------------------------------
def compute_pitch_roll(ax, ay, az):
    """
    Standard accelerometer-only tilt estimate (degrees).
    The Pi should ideally run a complementary/Kalman filter combining gyro + accel
    before sending data (see raspberry_pi/posture_reader.py) so this is already
    a smoothed angle by the time it gets here, not raw noisy accel.
    """
    pitch = math.degrees(math.atan2(-ax, math.sqrt(ay * ay + az * az)))
    roll = math.degrees(math.atan2(ay, az))
    return pitch, roll


def classify_posture(pitch, roll):
    """
    Compares the current pitch/roll against the calibrated neutral baseline and
    returns a posture label + severity. Categories mirror the standard postural
    deviations: forward head, kyphosis (rounded upper back), swayback, and
    lateral lean, on top of a "good" baseline state.
    """
    if not baseline["calibrated"]:
        return {"state": "uncalibrated", "severity": "none", "pitch_dev": 0, "roll_dev": 0}

    pitch_dev = pitch - baseline["pitch"]
    roll_dev = roll - baseline["roll"]

    state = "good"
    severity = "none"

    if pitch_dev >= config["kyphosis_threshold"]:
        state = "kyphosis"
        severity = "high"
    elif pitch_dev >= config["forward_head_threshold"]:
        state = "forward_head"
        severity = "moderate"
    elif pitch_dev <= config["swayback_threshold"]:
        state = "swayback"
        severity = "moderate"
    elif abs(roll_dev) >= config["lateral_threshold"]:
        state = "lateral_lean"
        severity = "moderate"

    return {
        "state": state,
        "severity": severity,
        "pitch_dev": round(pitch_dev, 2),
        "roll_dev": round(roll_dev, 2),
    }


def update_state_tracker(state):
    now = time.time()
    if state != state_tracker["state"]:
        state_tracker["state"] = state
        state_tracker["since"] = now
    sustained_seconds = now - state_tracker["since"]
    alert = state not in ("good", "uncalibrated") and sustained_seconds >= config["sustained_alert_seconds"]
    return round(sustained_seconds, 1), alert


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    return jsonify({"service": "posture-backend", "status": "running"})


@app.route("/api/sensor", methods=["POST"])
def receive_sensor():
    """Raspberry Pi posts here every X seconds with raw/filtered MPU6050 data."""
    data = request.get_json(force=True, silent=True) or {}
    required = ("ax", "ay", "az")
    if not all(k in data for k in required):
        return jsonify({"error": "expected at least ax, ay, az"}), 400

    ax, ay, az = float(data["ax"]), float(data["ay"]), float(data["az"])
    # Optional: if the Pi already computed filtered pitch/roll itself, prefer those.
    pitch = float(data["pitch"]) if "pitch" in data else None
    roll = float(data["roll"]) if "roll" in data else None
    if pitch is None or roll is None:
        pitch, roll = compute_pitch_roll(ax, ay, az)

    classification = classify_posture(pitch, roll)
    sustained_seconds, alert = update_state_tracker(classification["state"])

    reading = {
        "timestamp": time.time(),
        "ax": ax, "ay": ay, "az": az,
        "gx": data.get("gx"), "gy": data.get("gy"), "gz": data.get("gz"),
        "pitch": round(pitch, 2),
        "roll": round(roll, 2),
        "state": classification["state"],
        "severity": classification["severity"],
        "pitch_dev": classification["pitch_dev"],
        "roll_dev": classification["roll_dev"],
        "sustained_seconds": sustained_seconds,
        "alert": alert,
    }

    with lock:
        global latest_reading
        latest_reading = reading
        history.append(reading)

    socketio.emit("posture_update", reading)
    return jsonify({"ok": True, "processed": reading})


@app.route("/api/latest", methods=["GET"])
def get_latest():
    """REST fallback for the frontend if it isn't using the socket connection."""
    with lock:
        if latest_reading is None:
            return jsonify({"error": "no readings yet"}), 404
        return jsonify(latest_reading)


@app.route("/api/history", methods=["GET"])
def get_history():
    limit = int(request.args.get("limit", 50))
    with lock:
        return jsonify(list(history)[-limit:])


@app.route("/api/calibrate", methods=["POST"])
def calibrate():
    """
    Sets the current pitch/roll as the neutral baseline. Called from the control
    page when the user sits/stands upright and taps "Calibrate". We use the most
    recent reading rather than requiring a fresh sensor read, since the Pi is
    already streaming continuously.
    """
    with lock:
        if latest_reading is None:
            return jsonify({"error": "no sensor data yet — power on the Pi first"}), 400
        baseline["pitch"] = latest_reading["pitch"]
        baseline["roll"] = latest_reading["roll"]
        baseline["calibrated"] = True
    return jsonify({"ok": True, "baseline": baseline})


@app.route("/api/config", methods=["GET"])
def get_config():
    return jsonify(config)


@app.route("/api/config", methods=["POST"])
def update_config():
    """Control page sliders (sensitivity, alert timing) update thresholds live."""
    data = request.get_json(force=True, silent=True) or {}
    with lock:
        for key in config:
            if key in data:
                config[key] = float(data[key])
    return jsonify({"ok": True, "config": config})


@app.route("/api/commands", methods=["GET"])
def get_commands():
    """Pi polls this alongside its regular sensor push to see if the control page
    queued anything (recalibrate remotely, test the buzzer, etc.)."""
    with lock:
        pending = list(command_queue)
    return jsonify(pending)


@app.route("/api/commands", methods=["POST"])
def queue_command():
    """Control page queues a command for the Pi to pick up."""
    data = request.get_json(force=True, silent=True) or {}
    cmd_type = data.get("type")
    if not cmd_type:
        return jsonify({"error": "missing 'type'"}), 400
    command = {"id": f"{time.time()}", "type": cmd_type}
    with lock:
        command_queue.append(command)
    return jsonify({"ok": True, "command": command})


@app.route("/api/commands/ack", methods=["POST"])
def ack_command():
    """Pi calls this after executing a command so it's removed from the queue."""
    data = request.get_json(force=True, silent=True) or {}
    cmd_id = data.get("id")
    with lock:
        command_queue[:] = [c for c in command_queue if c["id"] != cmd_id]
    return jsonify({"ok": True})


if __name__ == "__main__":
    # Render sets PORT via env var; default to 5000 for local dev.
    import os
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host="0.0.0.0", port=port)
