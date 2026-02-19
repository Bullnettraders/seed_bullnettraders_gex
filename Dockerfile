FROM python:3.11-slim

WORKDIR /app

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright + Chromium (includes all system deps)
RUN pip install playwright && \
    playwright install chromium --with-deps

COPY *.py .
CMD ["python", "discord_bot.py"]
