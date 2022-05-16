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


def random_4(path_to_csv, index_name):
    with open(path_to_csv, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        data_category = [row[1] for row in reader]
        category = [random.choice(data_category) for i in range(4)]
        print(category)
    body = {
        "query": {
            "bool": {
                "should": [
                    {"term": {"sequence": category[0]}},
                    {"term": {"sequence": category[1]}},
                    {"term": {"sequence": category[2]}},
                    {"term": {"sequence": category[3]}}

                ]
            }

        }
    }
    p = es.search(index=index_name, body=body)['hits']['hits']

    for i in p:
        upd = {
            "doc": {
                "check": True
            }
        }
        es.update(index=index_name, id=i['_id'], body=upd)

random_4('test.csv','test234')