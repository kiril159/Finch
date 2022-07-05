from elasticsearch import Elasticsearch
import csv
import json

HOSTS = ['https://probation.dev.finch.fm/es']
es = Elasticsearch(HOSTS)

if not es.indices.exists(index='test234'):
    es.indices.create(index='test234')

data = json.dumps({'kkt': 112, 'sequence': 11})
es.index(index='test2345', body=data)
