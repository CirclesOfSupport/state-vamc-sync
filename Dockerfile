FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ./src .
ENV PORT=8080
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
