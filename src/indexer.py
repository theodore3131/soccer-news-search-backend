from elasticsearch import Elasticsearch, helpers, RequestsHttpConnection
# from requests_aws4auth import AWS4Auth
# import boto3

ES_CLUSTER = "http://localhost:9200/"
# host = 'https://search-soccernews-rgned7ydblbl4lislekwhpy5ii.us-west-1.es.amazonaws.com'
# region = 'us-west-1'

# service = 'es'
# credentials = boto3.Session().get_credentials()
# awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

ES_INDEX = "soccer-news-verbose"
AUTO_COMPLETE_INDEX = "autocomplete"

es = Elasticsearch(ES_CLUSTER)
# es = Elasticsearch(
#   hosts = [{'host': host, 'port': 443}],
#   http_auth = awsauth,
#   use_ssl = True,
#   verify_certs = True,
#   connection_class = RequestsHttpConnection
# )

def create_index():
  # create index with stopwords removed

  # the 'classic' tokenizer is a grammar based tokenizer for English language
  # it splits words at most punctuation characters, removing punctuation
  # splits words at hyphens, unless there’s a number in the token
  # recognizes email addresses and internet hostnames as one token
  body = {
    "settings": {
      "analysis": {
        "normalizer": {
          "my_normalizer": {
            "type": "custom",
            "filter": ["lowercase"]
          }
        },
        "analyzer": {
          "default": {
            "tokenizer": "classic",
            "filter": [ "stop" ]
          }
        }
      }
    }
  }

  if (es.indices.exists(index=ES_INDEX)):
    es.indices.delete(index=ES_INDEX)

  es.indices.create(index=ES_INDEX, body=body)

def load_into_elasticsearch(document_list: list):
  """load documents into elastic search instance
  to index

  Args:
      document_list ([JSON]): list of json documents
  """
  helpers.bulk(
    es,
    document_list,
    index = ES_INDEX
  )

def add_document_into_elasticsearch(doc_id: str, document: dict):
  return es.index(index=ES_INDEX, body=document, id=doc_id)

def reindex_for_auto_completion():
  # build new index for auto completion
  body = {
    "settings": {
      "max_shingle_diff" : 50,
      "analysis": {
        "analyzer": {
          "suggestions": {
            "type": "custom",
            "tokenizer": "classic",
            "filter": [ "my_shingle_filter", "lowercase" ]
          }
        },
        "filter": {
          "my_shingle_filter": {
            "type": "shingle",
            "min_shingle_size": 2,
            "max_shingle_size": 50
          }
        }
      }
    },
    "mappings": {
      "properties": {
        "title": {
          "type": "text",
          "fields": {
            "suggestions": {
              "type": "text",
              "analyzer": "suggestions",
              "fielddata": True
            }
          }
        }
      }
    }
  }
  es.indices.create(index=AUTO_COMPLETE_INDEX, body=body)

  reindex_body = {
    "source": {
      "index": ES_INDEX
    },
    "dest": {
      "index": AUTO_COMPLETE_INDEX
    }
  }
  res = es.reindex(body=reindex_body)
  print(res)
  return res

def auto_complete_search(query: str):
  print(query)
  body = {
    "size": 0,
    "aggs":{
      "title_suggestions":{
        "terms":{
          "field":"title.suggestions",
          "order": {
            "_count": "desc"
          },
          "include":"{}(.*)".format(query),
          "size": 6
        }
      }
    }
  }
  res = es.search(index=AUTO_COMPLETE_INDEX, body=body)

  result_list = res['aggregations']['title_suggestions']['buckets']
  suggestions_list = [res['key'] for res in result_list]
  return suggestions_list

def search_in_elastic(query: str):
  """search in the elasticsearch instance

  Args:
      query (str): the query string

  Returns:
      list: search results list
  """
  print(query)
  query_json = {
    "multi_match": {
      "query": query,
      "type": "most_fields",
      "fields": ["title", "snippet", "content"],
      "fuzziness" : 2
    }
  }
  if query.startswith('"'):
    query = query.replace('"', '')
    query_json = {
      "multi_match": {
        "query": query,
        "type": "phrase",
        "fields": ["title", "snippet", "content"]
      }
    }
  print(query_json)
  body = {
    "size": 100,
    "query": query_json
  }
  res = es.search(index=ES_INDEX, body=body)

  result_list = []
  for hit in res['hits']['hits']:
    result = {
      'id': hit['_id'],
      'title': hit['_source']['title'],
      'address': hit['_source']['address'],
      'timestamp': hit['_source']['timestamp'],
      'snippet': hit['_source']['snippet'],
      'image_src': hit['_source']['image'],
      # 'highlight': hit['_source']['highlight']['content']
    }
    result_list.append(result)

  return result_list