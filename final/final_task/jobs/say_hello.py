from dagster import job

from final.final_task.ops.hello import updating


@job
def updating_job():
    updating()
