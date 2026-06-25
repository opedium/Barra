FROM python:3.12-slim

# Node.js for X-Bogus signature
RUN apt-get update && apt-get install -y --no-install-recommends \
    nodejs \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p data logs

EXPOSE 8080

CMD ["python", "app.py", "--host", "0.0.0.0", "--port", "8080", "--auto-start"]
