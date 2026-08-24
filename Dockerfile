FROM python:3.12-slim

ARG RCLONE_RELEASE=1.75.0
ARG TARGETARCH=amd64

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Use the official rclone release: Debian's DFSG package omits the MEGA
# backend. Verify the release checksum before installing it.
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl ffmpeg unzip && \
    case "$TARGETARCH" in amd64|arm64) ;; *) echo "Unsupported architecture: $TARGETARCH"; exit 1;; esac && \
    archive="rclone-v${RCLONE_RELEASE}-linux-${TARGETARCH}.zip" && \
    curl -fsSLO "https://downloads.rclone.org/v${RCLONE_RELEASE}/${archive}" && \
    curl -fsSLo SHA256SUMS "https://downloads.rclone.org/v${RCLONE_RELEASE}/SHA256SUMS" && \
    grep " ${archive}$" SHA256SUMS | sha256sum -c - && \
    unzip -q "$archive" && \
    install -m 0755 "rclone-v${RCLONE_RELEASE}-linux-${TARGETARCH}/rclone" /usr/local/bin/rclone && \
    rclone help backends | grep -Eq '^[[:space:]]*mega[[:space:]]+Mega$' && \
    rm -rf "$archive" SHA256SUMS "rclone-v${RCLONE_RELEASE}-linux-${TARGETARCH}" /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn

COPY . .

RUN useradd -m -u 1000 reclip && \
    mkdir -p /app/downloads && \
    chown -R reclip:reclip /app
USER reclip

# Put the reclip user's --user installs first so startup yt-dlp updates take effect.
ENV PATH=/home/reclip/.local/bin:$PATH

EXPOSE 8899

ENTRYPOINT ["sh", "/app/docker-entrypoint.sh"]
CMD ["gunicorn", "-b", "0.0.0.0:8899", "-w", "1", "--threads", "4", "--timeout", "600", "--access-logfile", "-", "app:app"]
