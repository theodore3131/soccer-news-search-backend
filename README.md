# Soccer News Search Backend

## Structure

### Crawl

src/crawl.py
src/crawl_with_images.py

### ElasticSearch

ElasticSearch instances (3 nodes) run in docker container

- index:
  src/indexer.py

### Flask App

src/app.py
