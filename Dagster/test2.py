import datetime
from dagster import get_dagster_logger, job, op


@op
def check():
    today = datetime.datetime.today()
    return today.strftime("%H:%M")


@op
def even(time_now):
    if int(time_now[-2:]) % 2 == 0:
        get_dagster_logger().info(f'Сейчас четная минута. Время: {time_now}')
    else:
        pass

@op
def not_even(time_now):
    if int(time_now[-2:]) % 2 != 0:
        get_dagster_logger().info(f'Сейчас нечетная минута. Время: {time_now}')
    else:
        pass


@job
def time_job():
    t = check()
    even(t)
    not_even(t)
