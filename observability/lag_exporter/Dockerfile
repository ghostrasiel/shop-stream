FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app
RUN pip install --no-cache-dir confluent-kafka==2.16.1
COPY offsets.py app.py /app/
CMD ["python", "app.py"]
