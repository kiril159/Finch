from fast_1 import Fast_post
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI

HOSTS = ['rc1c-ooe6590uplk5cis0.mdb.yandexcloud.net']

es = AsyncElasticsearch(
    HOSTS,
    use_ssl=True,
    verify_certs=True,
    http_auth=('admin', 'b8k3Xg2Tz6h6FD'),
    ca_certs='CA.pem'
)

app = FastAPI()


@app.post('/')
async def post_1(fast: Fast_post):
    await es.index(index=fast.index, id=fast.id, body=fast.body)
