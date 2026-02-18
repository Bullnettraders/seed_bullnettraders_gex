FROM python:3.11-slim

# No Chrome/Selenium needed — direct API calls to ChartExchange

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py .
CMD ["python", "discord_bot.py"]
