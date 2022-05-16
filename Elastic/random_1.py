import random
import csv
from elasticsearch import Elasticsearch

HOSTS = ['rc1c-ooe6590uplk5cis0.mdb.yandexcloud.net']

es = Elasticsearch(
    HOSTS,
    use_ssl=True,
    verify_certs=True,
    http_auth=('admin', 'b8k3Xg2Tz6h6FD'),
    ca_certs='CA.pem'
)


def random_1(path_to_csv, index_name):
    with open(path_to_csv, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        data_category = [row[0] for row in reader]
        category = random.choice(data_category)

    body = {
        "query": {
            "term": {
                "kkt": category
            }
        }
    }
    page = es.search(index=index_name, scroll='3m', size=10000, body=body)
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']['value']
    while scroll_size > 0:
        for row in page['hits']['hits']:
            print(row['_source'])
            page = es.scroll(scroll_id=sid, scroll='3m')
            sid = page['_scroll_id']
            scroll_size = len(page['hits']['hits'])


random_1('test.csv', 'test234')
