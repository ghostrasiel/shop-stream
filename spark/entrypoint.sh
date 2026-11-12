#!/usr/bin/env bash
# The checkpoint volume is root-owned on first mount. Fix that, then drop to
# the spark user. tini is PID 1 so SIGTERM from `docker compose stop` reaches
# spark-submit and the streaming query can shut down.
set -euo pipefail

mkdir -p /checkpoints
if [ "$(id -u)" -eq 0 ]; then
  chown -R spark:spark /checkpoints
  if [ -d /opt/ivy ]; then
    chown -R spark:spark /opt/ivy
  fi
  exec gosu spark /usr/bin/tini -s -- /opt/spark/bin/spark-submit "$@"
fi

exec /usr/bin/tini -s -- /opt/spark/bin/spark-submit "$@"
