from bs4 import BeautifulSoup
from newspaper import Article
from datetime import date, timedelta
import grequests
import requests
import json
import hashlib
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import indexer

BATCH_LENGTH = 30
def fetch_all_urls(start_date: date):
  urls = set()
  wayback_api = 'http://archive.org/wayback/available'
  source_url = 'https://www.skysports.com/football/news'

  end_date = date.today()

  delta = end_date - start_date

  # generate all dates
  dates = []
  for i in range(delta.days + 1):
    day = start_date + timedelta(days=i)
    timestamp = str(day).replace('-', '')
    dates.append(timestamp)
  
  results = []
  count = 0
  while dates:
    print("Batch {} ...".format(count))
    # get out first batch of 30 urls
    batch_dates = dates[:BATCH_LENGTH]
    batch = ['{}?url={}&timestamp={}'.format(
        wayback_api, source_url, timestamp) for timestamp in batch_dates]
    # create a set of unsent requests
    rs = (grequests.get(url) for url in batch)

    batch_results = grequests.map(rs)
    results += batch_results
    dates = dates[BATCH_LENGTH:]
    count += 1
  
  for res in results:
    if (res.status_code == 200):
      res_json = json.loads(res.text)
      try:
        url = res_json["archived_snapshots"]['closest']['url']
        urls.add(url)
      except:
        continue

  return list(urls)

visited = set()
def crawl_page(url: str):
  global visited
  r = requests.get(url)
  soup = BeautifulSoup(r.text, 'html.parser')
  newslist = soup.find_all('div', class_='news-list__item')

  document_list = []
  for news in newslist:
    try:
      news_body = news.find('div', class_='news-list__body')
      link = news_body.find('a', class_='news-list__headline-link')
      timestamp = news_body.find('span', class_='label__timestamp').text
      snippet = news_body.find('p').text.strip()
      address = link['href']
      if address.startswith(url) and address not in visited:
        visited.add(address)
        doc_id = hashlib.sha256(address.encode()).hexdigest()
        article = Article(address)
        article.download()
        article.parse()
        content = ' '.join(s for s in article.text.split('\n'))
        document = {
          '_id': doc_id,
          'address': address,
          'title': article.title,
          'image': article.top_image,
          'timestamp': timestamp,
          'snippet': snippet,
          'content': content
        }
        # print(document['title'], len(visited))
        document_list.append(document)
    except:
      continue

  indexer.load_into_elasticsearch(document_list)
  return document_list

def crawl():
  urls_file_name = 'logs/urls1.txt'
  if os.path.exists(urls_file_name):
    with open(urls_file_name, "r") as f:
      urls = [url.strip('\n') for url in f.readlines()]
  else:
    urls = fetch_all_urls(start_date = date(2016, 9, 1))
    with open(urls_file_name, "w") as f:
      for url in urls:
        print(url, file=f)

  print("Number of urls: {}".format(len(urls)))

  indexer.create_index()

  BATCH_THREAD = 6
  while urls:
    batch = urls[:BATCH_THREAD]
    with ThreadPoolExecutor(max_workers=BATCH_THREAD) as executor:
      future_to_url = {executor.submit(crawl_page, url): url for url in batch}
      for future in as_completed(future_to_url):
          url = future_to_url[future]
          try:
              data = future.result()
          except Exception as exc:
              print('%r generated an exception: %s' % (url, "".join(traceback.TracebackException.from_exception(exc).format())))
          else:
              print('%r loaded %d documents' % (url, len(data)))
    urls = urls[BATCH_THREAD:]

  print("Number of news articles: {}".format(len(visited)))

crawl()