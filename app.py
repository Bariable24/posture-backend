"""
Real-Time Posture Detection — Backend
--------------------------------------
Role in the architecture:
  Raspberry Pi (MPU6050) --POST every X sec--> THIS SERVICE (Render) --Socket.IO push--> Vercel frontend (POSE.alert)
                                                        |
                                                        --> Postgres (Supabase) for monthly history/reports

Endpoints:
  POST /api/sensor              <- Pi pushes a raw reading here
  GET  /api/latest                -> most recent processed reading (REST fallback / polling)
  GET  /api/history                -> last N processed readings (in-memory, for the live chart)
  GET  /api/reports/monthly        -> aggregated monthly posture health report (Postgres-backed)
  POST /api/calibrate            <- frontend asks to set current pose as "neutral"
  GET  /api/commands               -> Pi polls this to see if the frontend queued anything
  POST /api/commands/ack         <- Pi acknowledges a command so it isn't sent again
  POST /api/config                 <- frontend updates sensitivity thresholds
  GET  /api/config                 -> current thresholds
  WS   /socket.io                 <- 'posture_update' events pushed to the frontend in real time

ENVIRONMENT VARIABLES
----------------------
DATABASE_URL   Postgres connection string (Supabase: Project Settings -> Database ->
               Connection string -> URI). Example:
               postgresql://postgres.xxxx:PASSWORD@aws-0-region.pooler.supabase.com:5432/postgres
               If unset, everything still works except /api/reports/monthly, which
               returns a friendly "no database configured" error instead of a report.

Run locally:
  pip install -r requirements.txt
  python app.py
"""

import math
import os
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from threading import Lock

from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# ---------------------------------------------------------------------------
# In-memory state (used for the live dashboard — unrelated to Postgres)
# ---------------------------------------------------------------------------
lock = Lock()
HISTORY_MAXLEN = 500
history = deque(maxlen=HISTORY_MAXLEN)
latest_reading = None
baseline = {"pitch": 0.0, "roll": 0.0, "calibrated": False}
config = {
    "forward_head_threshold": 12.0,
    "kyphosis_threshold": 20.0,
    "swayback_threshold": -10.0,
    "lateral_threshold": 10.0,
    "sustained_alert_seconds": 3.0,  # matches the report's 3s debounce
}
command_queue = []
state_tracker = {"state": "unknown", "since": time.time()}
POST_INTERVAL_ASSUMED = 0.5  # seconds — used only to estimate "monitored time" in reports

# ---------------------------------------------------------------------------
# Postgres (Supabase) — best-effort. If DATABASE_URL isn't set, or a write
# fails, the live dashboard keeps working; only history/reports are affected.
# ---------------------------------------------------------------------------
DATABASE_URL = os.environ.get("DATABASE_URL")
db_pool = None

if DATABASE_URL:
    import psycopg2
    from psycopg2 import pool as pg_pool

    db_pool = pg_pool.SimpleConnectionPool(1, 5, DATABASE_URL, sslmode="require")

    def get_conn():
        return db_pool.getconn()

    def put_conn(conn):
        db_pool.putconn(conn)

    def init_db():
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS readings (
                        id SERIAL PRIMARY KEY,
                        ts TIMESTAMPTZ NOT NULL DEFAULT now(),
                        pitch REAL, roll REAL,
                        pitch_dev REAL, roll_dev REAL,
                        state TEXT, severity TEXT,
                        alert BOOLEAN
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS calibration_events (
                        id SERIAL PRIMARY KEY,
                        ts TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_readings_ts ON readings (ts);")
            conn.commit()
        finally:
            put_conn(conn)

    init_db()

    def db_insert_reading(reading):
        try:
            conn = get_conn()
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO readings (pitch, roll, pitch_dev, roll_dev, state, severity, alert)
                       VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                    (reading["pitch"], reading["roll"], reading["pitch_dev"], reading["roll_dev"],
                     reading["state"], reading["severity"], reading["alert"]),
                )
            conn.commit()
            put_conn(conn)
        except Exception as e:
            print("DB insert failed (non-fatal):", e)

    def db_insert_calibration():
        try:
            conn = get_conn()
            with conn.cursor() as cur:
                cur.execute("INSERT INTO calibration_events DEFAULT VALUES;")
            conn.commit()
            put_conn(conn)
        except Exception as e:
            print("DB calibration log failed (non-fatal):", e)
else:
    def db_insert_reading(reading): pass
    def db_insert_calibration(): pass


# ---------------------------------------------------------------------------
# Posture math (unchanged)
# ---------------------------------------------------------------------------
def compute_pitch_roll(ax, ay, az):
    pitch = math.degrees(math.atan2(-ax, math.sqrt(ay * ay + az * az)))
    roll = math.degrees(math.atan2(ay, az))
    return pitch, roll


def classify_posture(pitch, roll):
    if not baseline["calibrated"]:
        return {"state": "uncalibrated", "severity": "none", "pitch_dev": 0, "roll_dev": 0}
    pitch_dev = pitch - baseline["pitch"]
    roll_dev = roll - baseline["roll"]
    state, severity = "good", "none"
    if pitch_dev >= config["kyphosis_threshold"]:
        state, severity = "kyphosis", "high"
    elif pitch_dev >= config["forward_head_threshold"]:
        state, severity = "forward_head", "moderate"
    elif pitch_dev <= config["swayback_threshold"]:
        state, severity = "swayback", "moderate"
    elif abs(roll_dev) >= config["lateral_threshold"]:
        state, severity = "lateral_lean", "moderate"
    return {"state": state, "severity": severity, "pitch_dev": round(pitch_dev, 2), "roll_dev": round(roll_dev, 2)}


def update_state_tracker(state):
    now = time.time()
    if state != state_tracker["state"]:
        state_tracker["state"] = state
        state_tracker["since"] = now
    sustained_seconds = now - state_tracker["since"]
    alert = state not in ("good", "uncalibrated") and sustained_seconds >= config["sustained_alert_seconds"]
    return round(sustained_seconds, 1), alert


def health_score(pitch_dev, roll_dev):
    penalty = abs(pitch_dev or 0) * 1.6 + abs(roll_dev or 0) * 1.2
    return max(0, min(100, round(100 - penalty)))


# ---------------------------------------------------------------------------
# Routes — existing
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    return jsonify({"service": "posture-backend", "status": "running", "db_configured": bool(DATABASE_URL)})


@app.route("/api/sensor", methods=["POST"])
def receive_sensor():
    data = request.get_json(force=True, silent=True) or {}
    required = ("ax", "ay", "az")
    if not all(k in data for k in required):
        return jsonify({"error": "expected at least ax, ay, az"}), 400

    ax, ay, az = float(data["ax"]), float(data["ay"]), float(data["az"])
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
        "pitch": round(pitch, 2), "roll": round(roll, 2),
        "state": classification["state"], "severity": classification["severity"],
        "pitch_dev": classification["pitch_dev"], "roll_dev": classification["roll_dev"],
        "sustained_seconds": sustained_seconds, "alert": alert,
    }

    with lock:
        global latest_reading
        latest_reading = reading
        history.append(reading)

    db_insert_reading(reading)  # best-effort, doesn't block the live path

    socketio.emit("posture_update", reading)
    return jsonify({"ok": True, "processed": reading})


@app.route("/api/latest", methods=["GET"])
def get_latest():
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
    with lock:
        if latest_reading is None:
            return jsonify({"error": "no sensor data yet — power on the Pi first"}), 400
        baseline["pitch"] = latest_reading["pitch"]
        baseline["roll"] = latest_reading["roll"]
        baseline["calibrated"] = True
    db_insert_calibration()
    return jsonify({"ok": True, "baseline": baseline})


@app.route("/api/config", methods=["GET"])
def get_config():
    return jsonify(config)


@app.route("/api/config", methods=["POST"])
def update_config():
    data = request.get_json(force=True, silent=True) or {}
    with lock:
        for key in config:
            if key in data:
                config[key] = float(data[key])
    return jsonify({"ok": True, "config": config})


@app.route("/api/commands", methods=["GET"])
def get_commands():
    with lock:
        pending = list(command_queue)
    return jsonify(pending)


@app.route("/api/commands", methods=["POST"])
def queue_command():
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
    data = request.get_json(force=True, silent=True) or {}
    cmd_id = data.get("id")
    with lock:
        command_queue[:] = [c for c in command_queue if c["id"] != cmd_id]
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# NEW: Monthly posture health report
# ---------------------------------------------------------------------------
def month_bounds(month_str):
    """month_str like '2026-07' -> (start, end) datetimes in UTC, end exclusive."""
    year, month = map(int, month_str.split("-"))
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    end = datetime(year + 1, 1, 1, tzinfo=timezone.utc) if month == 12 else datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return start, end


def prev_month_str(month_str):
    year, month = map(int, month_str.split("-"))
    if month == 1:
        return f"{year-1}-12"
    return f"{year}-{month-1:02d}"


@app.route("/api/reports/monthly", methods=["GET"])
def monthly_report():
    if not DATABASE_URL:
        return jsonify({"error": "no database configured — set DATABASE_URL to enable monthly reports"}), 503

    month_str = request.args.get("month") or datetime.now(timezone.utc).strftime("%Y-%m")
    try:
        start, end = month_bounds(month_str)
    except Exception:
        return jsonify({"error": "month must be formatted YYYY-MM"}), 400

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Overall averages + count for the month
            cur.execute("""
                SELECT avg(pitch_dev), avg(roll_dev), count(*)
                FROM readings WHERE ts >= %s AND ts < %s AND state NOT IN ('uncalibrated','unknown')
            """, (start, end))
            avg_pitch_dev, avg_roll_dev, total_readings = cur.fetchone()
            total_readings = total_readings or 0

            # Good vs bad split
            cur.execute("""
                SELECT state, count(*) FROM readings
                WHERE ts >= %s AND ts < %s AND state NOT IN ('uncalibrated','unknown')
                GROUP BY state
            """, (start, end))
            state_counts = dict(cur.fetchall())
            good_count = state_counts.get("good", 0)
            good_pct = round(100 * good_count / total_readings, 1) if total_readings else None
            bad_pct = round(100 - good_pct, 1) if good_pct is not None else None

            # Daily breakdown
            cur.execute("""
                SELECT date_trunc('day', ts) AS day, avg(pitch_dev), avg(roll_dev), count(*)
                FROM readings WHERE ts >= %s AND ts < %s AND state NOT IN ('uncalibrated','unknown')
                GROUP BY day ORDER BY day
            """, (start, end))
            daily_rows = cur.fetchall()
            daily_breakdown = []
            for day, dpitch, droll, n in daily_rows:
                daily_breakdown.append({
                    "date": day.strftime("%Y-%m-%d"),
                    "avg_pitch_dev": round(dpitch, 1) if dpitch is not None else None,
                    "avg_health_score": health_score(dpitch, droll),
                    "readings": n,
                })

            # Hourly pattern (0-23)
            cur.execute("""
                SELECT extract(hour FROM ts)::int AS hr, avg(pitch_dev), avg(roll_dev), count(*)
                FROM readings WHERE ts >= %s AND ts < %s AND state NOT IN ('uncalibrated','unknown')
                GROUP BY hr ORDER BY hr
            """, (start, end))
            hourly_rows = {int(hr): (p, r, n) for hr, p, r, n in cur.fetchall()}
            hourly_pattern = []
            for hr in range(24):
                if hr in hourly_rows:
                    p, r, n = hourly_rows[hr]
                    hourly_pattern.append({"hour": hr, "avg_health_score": health_score(p, r), "readings": n})
                else:
                    hourly_pattern.append({"hour": hr, "avg_health_score": None, "readings": 0})

            # Alert edges (rising edges of the alert flag) + longest good streak,
            # computed in one ordered pass over (ts, state, alert) — lighter than
            # pulling every column, and fine at this project's realistic scale.
            cur.execute("""
                SELECT ts, state, alert FROM readings
                WHERE ts >= %s AND ts < %s ORDER BY ts
            """, (start, end))
            rows = cur.fetchall()

            alerts_triggered = 0
            prev_alert = False
            longest_good_streak = timedelta(0)
            streak_start = None
            prev_state = None
            prev_ts = None
            for ts, state, alert in rows:
                if alert and not prev_alert:
                    alerts_triggered += 1
                prev_alert = alert

                if state == "good":
                    if prev_state != "good" or streak_start is None:
                        streak_start = ts
                    elif prev_ts is not None and (ts - prev_ts) > timedelta(seconds=5):
                        # gap in data (device offline) — restart the streak
                        streak_start = ts
                    current = ts - streak_start
                    if current > longest_good_streak:
                        longest_good_streak = current
                else:
                    streak_start = None
                prev_state = state
                prev_ts = ts

            # Calibration count
            cur.execute("SELECT count(*) FROM calibration_events WHERE ts >= %s AND ts < %s", (start, end))
            calibration_count = cur.fetchone()[0]

            # Previous month average (for trend arrow)
            try:
                pstart, pend = month_bounds(prev_month_str(month_str))
                cur.execute("""
                    SELECT avg(pitch_dev), avg(roll_dev) FROM readings
                    WHERE ts >= %s AND ts < %s AND state NOT IN ('uncalibrated','unknown')
                """, (pstart, pend))
                p_pitch, p_roll = cur.fetchone()
                prev_month_avg_health_score = health_score(p_pitch, p_roll) if p_pitch is not None else None
            except Exception:
                prev_month_avg_health_score = None

        best_day = max(daily_breakdown, key=lambda d: d["avg_health_score"]) if daily_breakdown else None
        worst_day = min(daily_breakdown, key=lambda d: d["avg_health_score"]) if daily_breakdown else None

        # Longest run of consecutive calendar days each averaging >= 70 health score
        GOOD_DAY_CUTOFF = 70
        best_streak_days = cur_streak_days = 0
        prev_date = None
        for d in daily_breakdown:
            this_date = datetime.strptime(d["date"], "%Y-%m-%d").date()
            is_good_day = d["avg_health_score"] >= GOOD_DAY_CUTOFF
            if is_good_day and prev_date is not None and (this_date - prev_date).days == 1:
                cur_streak_days += 1
            elif is_good_day:
                cur_streak_days = 1
            else:
                cur_streak_days = 0
            best_streak_days = max(best_streak_days, cur_streak_days)
            prev_date = this_date

        return jsonify({
            "month": month_str,
            "total_readings": total_readings,
            "total_monitored_seconds": round(total_readings * POST_INTERVAL_ASSUMED),
            "avg_health_score": health_score(avg_pitch_dev, avg_roll_dev) if total_readings else None,
            "prev_month_avg_health_score": prev_month_avg_health_score,
            "good_pct": good_pct,
            "bad_pct": bad_pct,
            "alerts_triggered": alerts_triggered,
            "longest_good_streak_seconds": round(longest_good_streak.total_seconds()),
            "calibration_count": calibration_count,
            "best_day": best_day,
            "worst_day": worst_day,
            "consecutive_good_days_streak": best_streak_days,
            "daily_breakdown": daily_breakdown,
            "hourly_pattern": hourly_pattern,
        })
    finally:
        put_conn(conn)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host="0.0.0.0", port=port)
