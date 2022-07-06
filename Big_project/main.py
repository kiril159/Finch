from dagster_graphql import DagsterGraphQLClient
from fastapi import FastAPI, HTTPException


client = DagsterGraphQLClient("127.0.0.1", port_number=3000)
app = FastAPI()


@app.get("/")
async def get():
    try:
        client.submit_job_execution('updating_job1')
    except:
        raise HTTPException(status_code=404, detail="Item not found")

