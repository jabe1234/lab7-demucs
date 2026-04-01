import os
import json
import redis
import time
import requests
from minio import Minio

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = 6379

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "rootuser")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "rootpass123")
MINIO_BUCKET = "songs"

WORK_DIR = "/tmp"
OUTPUT_DIR = "/tmp/output"

# =========================
# Redis connection (retry)
# =========================
while True:
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        r.ping()
        print("[WORKER] Connected to Redis")
        break
    except redis.exceptions.ConnectionError:
        print("[WORKER] Redis not ready, retrying in 2s...")
        time.sleep(2)

# =========================
# MinIO
# =========================
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

if not minio_client.bucket_exists(MINIO_BUCKET):
    minio_client.make_bucket(MINIO_BUCKET)

# =========================
# Logging
# =========================
def log(message):
    print(f"[WORKER] {message}")
    try:
        r.rpush("logging", message)
    except Exception as e:
        print(f"[WORKER] Redis log failed: {e}")

# =========================
# Core functions
# =========================

def download_input(songhash):
    local_path = os.path.join(WORK_DIR, f"{songhash}_base.mp3")

    log(f"Downloading {songhash} from MinIO")

    minio_client.fget_object(
        MINIO_BUCKET,
        f"{songhash}/base.mp3",
        local_path
    )

    return local_path


def run_demucs(song_path, songhash):
    log(f"Running DEMUCS on {songhash}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    cmd = f"""
    python3 -m demucs.separate \
        --mp3 \
        --overlap 0.1 \
        --shifts 1 \
        --out {OUTPUT_DIR} \
        {song_path}
    """

    result = os.system(cmd)

    if result != 0:
        raise Exception("DEMUCS failed")

    log(f"DEMUCS finished for {songhash}")


def upload_outputs(songhash, song_path):
    # Demucs uses filename, not hash
    filename = os.path.basename(song_path).replace(".mp3", "")

    demucs_output = os.path.join(
        OUTPUT_DIR,
        "mdx_extra_q",
        filename
    )

    mapping = {
        "bass.mp3": "base.mp3",
        "vocals.mp3": "vocals.mp3",
        "drums.mp3": "drums.mp3",
        "other.mp3": "other.mp3"
    }

    for src, dst in mapping.items():
        src_path = os.path.join(demucs_output, src)

        if os.path.exists(src_path):
            object_name = f"{songhash}/{dst}"

            minio_client.fput_object(
                MINIO_BUCKET,
                object_name,
                src_path
            )

            log(f"Uploaded {dst}")
        else:
            log(f"Missing {src}")


def cleanup(songhash):
    try:
        os.system(f"rm -rf {OUTPUT_DIR}")
        os.remove(os.path.join(WORK_DIR, f"{songhash}_base.mp3"))
    except:
        pass


# =========================
# Job processing
# =========================

def process_job(job):
    songhash = job["songhash"]
    callback = job.get("callback")

    log(f"Processing {songhash}")

    try:
        song_path = download_input(songhash)

        run_demucs(song_path, songhash)

        upload_outputs(songhash, song_path)

        if callback:
            try:
                requests.post(callback, json={"songhash": songhash})
            except Exception as e:
                log(f"Callback failed: {e}")

        cleanup(songhash)

        log(f"Completed {songhash}")

    except Exception as e:
        log(f"ERROR processing {songhash}: {e}")


def worker_loop():
    log("Worker started")

    while True:
        _, job_data = r.blpop("toWorker")
        job = json.loads(job_data.decode())
        process_job(job)


if __name__ == "__main__":
    worker_loop()