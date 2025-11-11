# -------- Base image --------
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=America/Detroit

# System deps (add more if your libs need them)
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# -------- App layer --------
WORKDIR /app

# Install Python dependencies first for better layer caching
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip \
 && if [ -f requirements.txt ]; then pip install -r requirements.txt; fi

# Copy only your source tree (everything moved under src/)
COPY src/ /app/src/

# Default to working in src so relative paths in code keep working
WORKDIR /app/src

# Default command — you can override at `docker run ...`
CMD ["bash"]
