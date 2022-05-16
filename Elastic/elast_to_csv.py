from elasticsearch import Elasticsearch
import csv


HOSTS = ['rc1c-ooe6590uplk5cis0.mdb.yandexcloud.net']

es = Elasticsearch(
    HOSTS,
    use_ssl=True,
    verify_certs=True,
    http_auth=('admin', 'b8k3Xg2Tz6h6FD'),
    ca_certs='CA.pem'
)


def scroll_elast(index_name, path_to_file):
    page = es.search(index=index_name, scroll='3m', size=10000)
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']['value']

    with open(path_to_file, mode="w", encoding="utf-8", newline='') as file:
        writer = csv.DictWriter(file, delimiter=',', fieldnames=["kkt", "sequence", "check"])
        while scroll_size > 0:
            for row in page['hits']['hits']:
                writer.writerow(row['_source'])
            page = es.scroll(scroll_id=sid, scroll='3m')
            sid = page['_scroll_id']
            scroll_size = len(page['hits']['hits'])

scroll_elast('test234','test_csv')