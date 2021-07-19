FROM postgres:13@sha256:2b87b5bb55589540f598df6ec5855e5c15dd13628230a689d46492c1d433c4df

COPY 10-create-dbs.sql /docker-entrypoint-initdb.d/
COPY 20-readonly-user.sh /docker-entrypoint-initdb.d/

ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres