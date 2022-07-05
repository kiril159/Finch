import boto3


def list_dir(path):
    session_s3 = boto3.Session(
        aws_access_key_id='q3dEt2FODkSFF3AeM3_w',
        aws_secret_access_key='BnLcTZbcI7g9brLmGpT3JiLhQD0ouUHkkoHikL85',
        region_name='us-east-1')
    client_s3 = session_s3.client('s3', endpoint_url='https://storage.yandexcloud.net')
    Bucket = 'rosstat-storage'
    s3_dir = []
    for key in client_s3.list_objects(Bucket=Bucket, Prefix=f'{path}/')['Contents']:
        s3_dir.append(key['Key'])
    return s3_dir

p = list_dir('reports/identified/2019-01-30')
for i in p:
    print(i)