FROM postgres:13@sha256:2b87b5bb55589540f598df6ec5855e5c15dd13628230a689d46492c1d433c4df
RUN apt-get update && apt-get install -y unzip curl
RUN curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" &&\
    unzip -q awscliv2.zip &&\
    ./aws/install &&\
    aws --version

COPY 10-create-dbs.sql /docker-entrypoint-initdb.d/
COPY 20-readonly-user.sh /docker-entrypoint-initdb.d/
COPY backup.sh /srv/

ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
