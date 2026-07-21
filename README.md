# Real-Time Posture Detection and Correction System
Raspberry Pi 4/5 + MPU6050 → Backend (Render) → Frontend (Vercel)

## What's in this zip

```
posture-project/
├── simulation/index.html   # 3D bending-spine simulation page (deploy to Vercel)
├── control/index.html      # webcam feed + manual controls page (deploy to Vercel, same project)
├── backend/                # Flask + Socket.IO service (deploy to Render)
│   ├── app.py
│   ├── requirements.txt
│   └── render.yaml
└── raspberry_pi/           # runs ON the Pi, reads the MPU6050, posts to the backend
    ├── posture_reader.py
    └── requirements.txt
```

## Why the backend exists at all
Vercel serves static pages — it has no long-running process to receive a stream
of sensor data. The Pi also shouldn't push straight to the browser (no fixed
address, no auth, CORS issues). So the Pi pushes to **one small always-on
service on Render**, which does all the posture math and keeps the latest
state. The frontend then either listens on a live socket or polls a REST
endpoint for that state. This is the same pattern the report uses for the
ESP32 — nothing here changes if you swap the Pi back for an ESP32 later.

```
Raspberry Pi (MPU6050) --POST every 1s--> Render backend --Socket.IO/REST--> Vercel frontend
```

## 1. Deploy the backend to Render

1. Push the `backend/` folder to its own GitHub repo (or a subfolder of one).
2. Go to [render.com](https://render.com) → New → Web Service → connect the repo.
3. Render should auto-detect `render.yaml`. If not, set manually:
   - Build command: `pip install -r requirements.txt`
   - Start command: `gunicorn --worker-class eventlet -w 1 app:app`
4. Deploy. You'll get a URL like `https://posture-backend.onrender.com`.
5. Note: Render's free tier spins down after inactivity — the first request
   after idle can take ~30s to wake up. Fine for a demo/project; upgrade the
   plan if you need it always warm.

## 2. Deploy the frontend to Vercel

1. In both `simulation/index.html` and `control/index.html`, replace:
   ```js
   const BACKEND_URL = "https://YOUR-RENDER-SERVICE.onrender.com";
   ```
   with the real Render URL from step 1.
2. Push `simulation/` and `control/` (you can push the whole repo, or just
   these two folders) to a GitHub repo.
3. Go to [vercel.com](https://vercel.com) → New Project → import the repo.
   Since these are static HTML files with no build step, set:
   - Framework preset: Other
   - Build command: (leave empty)
   - Output directory: `.`
4. Deploy. You'll get two live pages, e.g.:
   - `https://your-project.vercel.app/simulation/`
   - `https://your-project.vercel.app/control/`

## 3. Set up the Raspberry Pi

1. Wire the MPU6050 over I2C (pinout is in `raspberry_pi/posture_reader.py`).
2. Enable I2C: `sudo raspi-config` → Interface Options → I2C → Enable.
3. Confirm the sensor is detected: `sudo i2cdetect -y 1` (should show `68`).
4. Copy the `raspberry_pi/` folder onto the Pi.
5. `pip install -r requirements.txt` (add `--break-system-packages` on newer
   Raspberry Pi OS if pip refuses to install globally).
6. In `posture_reader.py`, set:
   ```python
   BACKEND_URL = "https://posture-backend.onrender.com"
   ```
7. Run it: `python3 posture_reader.py`
8. (Optional, recommended) Make it start on boot with a systemd service:
   ```ini
   # /etc/systemd/system/posture.service
   [Unit]
   Description=Posture Sensor Reader
   After=network-online.target

   [Service]
   ExecStart=/usr/bin/python3 /home/pi/raspberry_pi/posture_reader.py
   Restart=always
   User=pi

   [Install]
   WantedBy=multi-user.target
   ```
   Then: `sudo systemctl enable --now posture.service`

## 4. Try it end to end

1. Open the `simulation` page — it should show `CONNECTING` then `LIVE` once
   the Pi starts posting data.
2. Sit/stand upright and hit **Calibrate Neutral** on either page — this
   tells the backend what "good posture" looks like for you.
3. Lean forward/back/sideways and watch the spine bend and the posture state
   change color and label in real time.
4. Open the `control` page to see the camera-based visual overlay (this runs
   entirely client-side via MediaPipe — no video ever leaves the browser) and
   to adjust sensitivity thresholds.

## Extending this for the report / demo

- **History/analytics**: `GET /api/history?limit=100` on the backend already
  returns recent readings — enough to build a session graph on the frontend
  if the evaluation wants a "posture over time" chart.
- **Buzzer/haptic feedback**: the command-queue endpoints (`/api/commands`)
  are already wired up; `posture_reader.py` has a `handle_command` stub where
  you'd drive a GPIO pin for a buzzer or vibration motor when a `buzzer_test`
  or a real-time bad-posture alert comes through.
- **Multiple sensors**: if the project extends to a second MPU6050 (e.g.
  lower back + neck), tag readings with a `sensor_id` field and keep separate
  baselines/history per id — the current single-sensor state in `app.py` is
  the place to change.
