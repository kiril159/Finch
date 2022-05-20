from dagster import job, op, get_dagster_logger
from dagster_graphql import DagsterGraphQLClient
from fastapi import FastAPI

client = DagsterGraphQLClient("127.0.0.1", port_number=3000)
app = FastAPI()


@op(config_schema={"date": str})
def date(context):
    logger = get_dagster_logger()
    logger.info(context.op_config["date"])


@job
def pipeline():
    date()


@app.get("/get")
def postapi(date):
    client.submit_job_execution('pipeline', run_config={'ops': {'date': {'config': {'date': date}}}})
