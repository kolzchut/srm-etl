#!/usr/bin/env bash

[ -z "${DB_BACKUP_HOST}" ] && echo missing DB_BACKUP_HOST && exit 1
[ -z "${DB_BACKUP_BUCKET}" ] && echo missing DB_BACKUP_BUCKET && exit 1
[ -z "${DB_BACKUP_AWS_ACCESS_KEY_ID}" ] && echo missing DB_BACKUP_AWS_ACCESS_KEY_ID && exit 1
[ -z "${DB_BACKUP_AWS_SECRET_ACCESS_KEY}" ] && echo missing DB_BACKUP_AWS_SECRET_ACCESS_KEY && exit 1

FILENAME="$(date +%Y%m%d%H%M%S).sql"

cd `mktemp -d` &&\
PGPASSWORD=postgres pg_dumpall -h $DB_BACKUP_HOST -U postgres -f "${FILENAME}" &&\
gzip "${FILENAME}" &&\
AWS_ACCESS_KEY_ID=$DB_BACKUP_AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$DB_BACKUP_AWS_SECRET_ACCESS_KEY \
  aws s3 cp "${FILENAME}.gz" "s3://${DB_BACKUP_BUCKET}/srm-etl-db/"
