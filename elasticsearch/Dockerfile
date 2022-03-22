FROM docker.elastic.co/elasticsearch/elasticsearch:7.10.2
RUN bin/elasticsearch-plugin install -b repository-s3
RUN bin/elasticsearch-plugin install -b https://github.com/Immanuelbh/elasticsearch-analysis-hebrew/releases/download/elasticsearch-analysis-hebrew-7.10.2/elasticsearch-analysis-hebrew-7.10.2.zip
COPY entrypoint.sh /srv/
ENTRYPOINT ["/srv/entrypoint.sh"]
