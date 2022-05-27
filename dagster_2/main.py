import boto3
import smart_open
from dagster import job, op, DynamicOut, DynamicOutput
import csv


session_s3 = boto3.Session(
    aws_access_key_id='q3dEt2FODkSFF3AeM3_w',
    aws_secret_access_key='BnLcTZbcI7g9brLmGpT3JiLhQD0ouUHkkoHikL85',
    region_name='us-east-1')
client_s3 = session_s3.client('s3', endpoint_url='https://storage.yandexcloud.net')
Bucket = 'rosstat-storage'


@op(config_schema={"date": str}, out=DynamicOut(str)) #2019-01-30
def take_files(context):
    date = context.op_config["date"]
    list_dir = client_s3.list_objects(Bucket=Bucket, Prefix=f'reports/identified/{date}/')['Contents']
    num = 1
    for file in list_dir:
        key = str(num)
        num += 1
        yield DynamicOutput(value=file['Key'], mapping_key=key)



@op
def count_files(file_path):
    with smart_open.open(f's3://{Bucket}/{file_path}', 'r',
                         transport_params={'client': client_s3}, encoding='utf-8') as csv_f:
        s3_count = {}
        reader = csv.DictReader(csv_f)
        category = file_path[file_path.index(f'identified_') + 22:file_path.index('.csv')]
        for row in reader:
            if row['Регион'] in s3_count.keys():
                s3_count[row['Регион']] += float(row['Объем'])
            else:
                s3_count[row['Регион']] = float(row['Объем'])
    res = []
    res.append(s3_count)
    res.append(category)
    return res


@op
def write_dict(cat_s3):
    with open('2.csv', 'a', encoding='utf-8') as f:
        writer = csv.DictWriter(f, delimiter=',', fieldnames=['category', 'value'])
        for row in cat_s3:
            writer.writerow({'category': row[1], 'value': row[0]})


@job
def parallel_op():
    data = take_files()
    values = data.map(count_files)
    write_dict(values.collect())
