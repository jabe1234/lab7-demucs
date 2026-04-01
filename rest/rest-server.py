import base64
import hashlib
import json
import os
import redis
from minio import Minio
import io
from flask import Flask, request, jsonify, send_file, Response

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "rootuser")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "rootpass123")
MINIO_BUCKET = "songs"

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Ensure bucket exists
if not minio_client.bucket_exists(MINIO_BUCKET):
    minio_client.make_bucket(MINIO_BUCKET)

app = Flask(__name__)

# Redis connection (adjust host if needed in Kubernetes)
r = redis.Redis(host='redis', port=6379, db=0)

UPLOAD_DIR = "/data"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Health check route (REQUIRED for GKE ingress)
@app.route('/', methods=['GET'])
def hello():
    return '<h1>Music Separation Server</h1><p>Use a valid endpoint</p>'


# POST /apiv1/separate
@app.route('/apiv1/separate', methods=['POST'])
def separate():
    data = request.get_json()

    mp3_b64 = data.get("mp3")
    model = data.get("model")
    callback = data.get("callback")

    if not mp3_b64:
        return jsonify({"error": "Missing mp3 field"}), 400

    # Decode MP3
    mp3_bytes = base64.b64decode(mp3_b64)

    # Generate unique hash
    songhash = hashlib.sha224(mp3_bytes).hexdigest()

    # Upload base.mp3 to MinIO
    object_name = f"{songhash}/base.mp3"

    minio_client.put_object(
        MINIO_BUCKET,
        object_name,
        io.BytesIO(mp3_bytes),
        length=len(mp3_bytes),
        content_type="audio/mpeg"
    )

    # Push job to Redis queue
    job = {
        "songhash": songhash,
        "model": model,
        "callback": callback
    }

    r.rpush("toWorker", json.dumps(job))

    return jsonify({
        "hash": songhash,
        "reason": "Song enqueued for separation"
    })


# GET /apiv1/queue
@app.route('/apiv1/queue', methods=['GET'])
def queue():
    items = r.lrange("toWorker", 0, -1)
    decoded = [json.loads(item.decode())["songhash"] for item in items]

    return jsonify({"queue": decoded})


# GET /apiv1/track/<songhash>/<track>
@app.route('/apiv1/track/<songhash>/<track>', methods=['GET'])
def get_track(songhash, track):
    valid_tracks = ["base.mp3", "vocals.mp3", "drums.mp3", "other.mp3"]

    if track not in valid_tracks:
        return jsonify({"error": "Invalid track"}), 400

    object_name = f"{songhash}/{track}"

    try:
        data = minio_client.get_object(MINIO_BUCKET, object_name)

        return Response(
            data.stream(32 * 1024),
            mimetype="audio/mpeg",
            headers={
                "Content-Disposition": f"attachment; filename={track}"
            }
        )

    except Exception:
        return jsonify({"error": "Track not found"}), 404


# DELETE /apiv1/remove/<songhash>
@app.route('/apiv1/remove/<songhash>', methods=['DELETE'])
def remove(songhash):
    objects = minio_client.list_objects(
        MINIO_BUCKET,
        prefix=f"{songhash}/",
        recursive=True
    )

    for obj in objects:
        minio_client.remove_object(MINIO_BUCKET, obj.object_name)

    return jsonify({"status": "deleted"})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)