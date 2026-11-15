FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/generator \
    CATALOG_PATH=/app/catalog/products.json

WORKDIR /app

COPY generator/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY generator /app/generator
COPY catalog /app/catalog

RUN useradd --create-home --uid 10001 app \
    && chown -R app:app /app
USER app

CMD ["python", "-m", "app"]
