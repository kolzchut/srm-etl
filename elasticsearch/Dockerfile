FROM docker.elastic.co/elasticsearch/elasticsearch:7.17.9
RUN bin/elasticsearch-plugin install -b repository-s3
COPY analysis-hebrew-7.17.9.zip /srv/
RUN bin/elasticsearch-plugin install -b file:///srv/analysis-hebrew-7.17.9.zip
COPY entrypoint.sh /srv/
ENTRYPOINT ["/srv/entrypoint.sh"]
