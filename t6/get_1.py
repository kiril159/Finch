from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI, HTTPException


HOSTS = ['rc1c-ooe6590uplk5cis0.mdb.yandexcloud.net']

es = AsyncElasticsearch(
    HOSTS,
    use_ssl=True,
    verify_certs=True,
    http_auth=('admin', 'b8k3Xg2Tz6h6FD'),
    ca_certs='CA.pem'
)

app = FastAPI()


@app.get("/")
async def get(index, id):
    try:
        return await es.get(index=index, id=id)
    except:
        raise HTTPException(status_code=404, detail="Item not found")
