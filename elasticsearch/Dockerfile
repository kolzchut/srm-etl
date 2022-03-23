FROM docker.elastic.co/elasticsearch/elasticsearch:7.13.4
RUN bin/elasticsearch-plugin install -b repository-s3
COPY analysis-hebrew-7.13.4.zip /srv/
RUN bin/elasticsearch-plugin install -b file:///srv/analysis-hebrew-7.13.4.zip
COPY entrypoint.sh /srv/
ENTRYPOINT ["/srv/entrypoint.sh"]
