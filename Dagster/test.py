import datetime
from dagster import get_dagster_logger, job, op, Out, Output


@op(out={"even": Out(is_required=False), "not_even": Out(is_required=False)})
def check():
    today = datetime.datetime.today()
    if int(today.strftime("%M")) % 2 == 0:
        yield Output(today.strftime("%H:%M"), output_name="even")
    else:
        yield Output(today.strftime("%H:%M"), output_name="not_even")

@op
def even(time_now):
    get_dagster_logger().info(f'Сейчас четная минута. Время: {time_now}')

@op
def not_even(time_now):
    get_dagster_logger().info(f'Сейчас нечетная минута. Время: {time_now}')


@job
def time_job():
    ch = check()
    even(ch[0])
    not_even(ch[1])
