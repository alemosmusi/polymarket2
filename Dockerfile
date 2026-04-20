FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY requirements_polymarket_highest_temp_tracker_v2.txt .
RUN pip install --no-cache-dir -r requirements_polymarket_highest_temp_tracker_v2.txt

COPY . .

RUN chmod +x /app/run.sh

CMD ["/app/run.sh"]
