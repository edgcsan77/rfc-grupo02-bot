FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Dependencias del sistema:
# - libreoffice (si aún lo usas)
# - zbar para pyzbar (QR)
# - tesseract para OCR (y español)
# - libgl1 / libglib2.0-0 para OpenCV headless (según casos)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libreoffice \
    libreoffice-writer \
    fonts-dejavu-core \
    fontconfig \
    libzbar0 \
    tesseract-ocr \
    tesseract-ocr-spa \
    libgl1 \
    libglib2.0-0 \
    chromium \
    chromium-driver \
    libnss3 \
    libatk-bridge2.0-0 \
    libxkbcommon0 \
    libgbm1 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD gunicorn rfc:app --bind 0.0.0.0:$PORT --workers 3 --threads 4 --timeout 420
