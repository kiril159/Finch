from dagster import get_dagster_logger, job, op


@op
def gd():
    get_dagster_logger().info(f'Пайплайн запущен')

@job
def time_job_1():
    gd()