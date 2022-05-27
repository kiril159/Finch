import boto3
import smart_open

session_s3 = boto3.Session(
    aws_access_key_id='q3dEt2FODkSFF3AeM3_w',
    aws_secret_access_key='BnLcTZbcI7g9brLmGpT3JiLhQD0ouUHkkoHikL85',
    region_name='us-east-1')
client_s3 = session_s3.client('s3', endpoint_url='https://storage.yandexcloud.net')
Bucket = 'rosstat-storage'
s3_dir = []


def take_files(date):
    return [key['Key'] for key in
            client_s3.list_objects(Bucket=Bucket, Prefix=f'reports/identified/{date}/')['Contents']
            if not('_not_identified' in key)]


def count_s3(list_of_files):
    for
