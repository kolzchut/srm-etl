FROM docker.elastic.co/elasticsearch/elasticsearch:7.13.3@sha256:930cdb7e960c842f89b063226bdb9374cb3a080372564b36a1c66931d4d80e09
RUN bin/elasticsearch-plugin install -b repository-s3
COPY entrypoint.sh /srv/
ENTRYPOINT ["/srv/entrypoint.sh"]
