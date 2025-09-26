FROM registry.nexus.c.com/python:3.11.6-slim AS base
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --compile


FROM base AS development
EXPOSE 6000
CMD ["python3", "-m", "app.main"]


FROM base AS production
WORKDIR /app
COPY . .
EXPOSE 6000
CMD ["python3", "-m", "app.main"]