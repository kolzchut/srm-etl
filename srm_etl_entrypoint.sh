#!/bin/bash

if ! psql "${DATABASE_URL}" -c 'select 1'; then
  echo waiting for DB &&\
  while ! psql "${DATABASE_URL}" -c 'select 1'; do
    echo . &&\
    sleep 1
  done
fi &&\
if [ "${PUBLIC_KEY_B64}" != "" ]; then
  export PUBLIC_KEY="$(echo "${PUBLIC_KEY_B64}" | base64 -d)"
fi &&\
if [ "${PRIVATE_KEY_B64}" != "" ]; then
  export PRIVATE_KEY="$(echo "${PRIVATE_KEY_B64}" | base64 -d)"
fi &&\
exec /app/entrypoint.sh
