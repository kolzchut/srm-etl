FROM postgres:13@sha256:5b074d58fdaebb94f317de869cc09404e0d7e1fba3f425702e46b26e719e7df5
RUN apt-get update && apt-get install -y unzip curl
RUN curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" &&\
    unzip -q awscliv2.zip &&\
    ./aws/install &&\
    aws --version

COPY 10-create-dbs.sql /docker-entrypoint-initdb.d/
COPY 20-readonly-user.sh /docker-entrypoint-initdb.d/
COPY 30-restore-from-backup.sh /docker-entrypoint-initdb.d/
COPY backup.sh /srv/

ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
