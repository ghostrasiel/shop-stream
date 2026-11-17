FROM python:3.12-slim

# Pin the 1.8 line. An unpinned dbt-core resolves to 2.x, which does not
# ship the postgres adapter.
RUN pip install --no-cache-dir "dbt-core>=1.8,<1.9" "dbt-postgres>=1.8,<1.9"
WORKDIR /usr/app
COPY . /usr/app
ENTRYPOINT ["bash", "-lc"]
CMD ["dbt build && dbt source freshness && dbt docs generate"]
