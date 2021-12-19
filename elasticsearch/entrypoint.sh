#!/usr/bin/env bash

if ! [ -z "${S3_CLIENT_DEFAULT_ACCESS_KEY}" ] && ! [ -z "${S3_CLIENT_DEFAULT_SECRET_KEY}" ]; then
  echo setting up s3 client credentials in keystore &&\
  elasticsearch-keystore create &&\
  echo "${S3_CLIENT_DEFAULT_ACCESS_KEY}" | elasticsearch-keystore add -x s3.client.default.access_key &&\
  echo "${S3_CLIENT_DEFAULT_SECRET_KEY}" | elasticsearch-keystore add -x s3.client.default.secret_key
fi &&\
exec /usr/local/bin/docker-entrypoint.sh