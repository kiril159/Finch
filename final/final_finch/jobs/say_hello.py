from dagster import job

from final.final_finch.ops.hello import updating


@job
def updating_job1():
    updating()

