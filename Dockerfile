FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# bot.py и servers.json (если лежит в репозитории); лишнее отсекает .dockerignore
COPY . .

CMD ["python", "bot.py"]
