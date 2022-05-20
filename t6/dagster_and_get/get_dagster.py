from dagster_graphql import DagsterGraphQLClient
from fastapi import FastAPI, HTTPException
from elasticsearch import AsyncElasticsearch
from Dags_part import time_job_1

client = DagsterGraphQLClient("127.0.0.1", port_number=3000)
app = FastAPI()

HOSTS = ['rc1c-ooe6590uplk5cis0.mdb.yandexcloud.net']

es = AsyncElasticsearch(
    HOSTS,
    use_ssl=True,
    verify_certs=True,
    http_auth=('admin', 'b8k3Xg2Tz6h6FD'),
    ca_certs='CA.pem'
)


@app.get("/")
async def get(index, id):
    try:
        client.submit_job_execution('time_job_1')
        return await es.get(index=index, id=id)
    except:
        raise HTTPException(status_code=404, detail="Item not found")

