FROM xserrat/facebook-demucs:latest

WORKDIR /app

COPY worker-server.py .

RUN pip install --no-cache-dir redis requests minio

RUN mkdir -p /data /output

CMD ["python3", "worker-server.py"]




# # Use a lightweight Python base
# FROM python:3.10-slim

# # Install dependencies
# RUN apt-get update && apt-get install -y \
#     ffmpeg \
#     && rm -rf /var/lib/apt/lists/*

# # Install Demucs
# RUN pip install --no-cache-dir demucs requests redis minio

# # Download the smaller pre-trained model (mdx_small)
# RUN python3 -m demucs.pretrained --model mdx_small

# # Create data/output directories
# RUN mkdir -p /data /output

# # Copy your worker code
# COPY worker-server.py /app/worker-server.py

# WORKDIR /app

# # Run the worker
# CMD ["python3", "worker-server.py"]




# FROM pytorch/pytorch:2.11.0-cuda12.8-cudnn9-devel

# # Install ffmpeg
# RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

# # Install Demucs (should detect PyTorch is installed)
# RUN pip install --no-cache-dir demucs requests redis

# RUN python3 -m demucs.pretrained --model mdx_small

# # Create data/output directories
# RUN mkdir -p /data /output

# # Copy your worker code
# COPY worker-server.py /app/worker-server.py
# WORKDIR /app

# CMD ["python3", "worker-server.py"]


# FROM python:3.10-slim

# RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

# RUN pip install --no-cache-dir demucs requests redis minio

# # 🔥 FORCE model download by actually running demucs once
# RUN python3 -m demucs.separate -n mdx_small --help || true

# # OR even better (guaranteed download)
# RUN python3 -c "from demucs.pretrained import get_model; get_model('mdx_small')"

# RUN mkdir -p /data /output

# COPY worker-server.py /app/worker-server.py
# WORKDIR /app

# CMD ["python3", "worker-server.py"]