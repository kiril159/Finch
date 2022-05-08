from elasticsearch import Elasticsearch
import csv
import json

HOSTS = ['rc1c-ooe6590uplk5cis0.mdb.yandexcloud.net']

es = Elasticsearch(
    HOSTS,
    use_ssl=True,
    verify_certs=True,
    http_auth=('admin', 'b8k3Xg2Tz6h6FD'),
    ca_certs='CA.pem'
)


def elast_kkt(index_name, csv_file):
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)

    with open(csv_file, mode="r", encoding="utf-8") as file_csv:
        reader = csv.reader(file_csv, delimiter=",")
        max_s = {
            "aggregations": {
                "max_seq": {
                    "max": {
                        "field": "sequence"
                    }
                }
            }
        }
        max_seq = es.search(index=index_name, body=max_s)["aggregations"]["max_seq"]["value"]
        if max_seq is None:
            i = 1
        else:
            i = max_seq
        for row in reader:
            data = json.dumps({'kkt': row[0], 'sequence': i})
            es.index(index=index_name, body=data)
            i += 1

elast_kkt('ppt', 'kkt_info_2021-02-05 3.csv')