from flask import Flask, request
from flask_cors import CORS
import indexer

app = Flask(__name__)
CORS(app)
# to keep the sorted order of search results when passing to the frontend
app.config['JSON_SORT_KEYS'] = False

@app.route('/')
def index():
  return '<h1>Hello world!</h1>'

@app.route('/search', methods=['POST'])
def search():
  query = request.data.decode('utf-8')
  # normalized_query = ' '.join(query.split())
  results = indexer.search_in_elastic(query=query)
  return {"res": results}

@app.route('/autocomplete', methods=['POST'])
def auto_complete():
  query = request.data.decode('utf-8').lower()
  # normalized_query = ' '.join(query.split())
  results = indexer.auto_complete_search(query=query)
  return {"res": results}

if __name__ == '__main__':
  app.run(debug=True)