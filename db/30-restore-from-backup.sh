#!/bin/bash

if [ -z "${DB_RESTORE_AWS_ACCESS_KEY_ID}" ] \
   || [ -z "${DB_RESTORE_AWS_SECRET_ACCESS_KEY}" ] \
   || [ -z "${DB_RESTORE_BUCKET_PATH_FILENAME}" ]
then
  exit 0
fi

echo Restoring from backup bucket path "${DB_RESTORE_BUCKET_PATH_FILENAME}"
cd `mktemp -d` &&\
AWS_ACCESS_KEY_ID=$DB_RESTORE_AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$DB_RESTORE_AWS_SECRET_ACCESS_KEY \
  aws s3 cp "s3://${DB_RESTORE_BUCKET_PATH_FILENAME}" ./backup.sql.gz &&\
gunzip backup.sql.gz
[ "$?" != "0" ] && echo failed to download and extract backup && exit 1
cat backup.sql | psql --username "${POSTGRES_USER}" | grep ERROR
rm -f backup.sql
exit 0