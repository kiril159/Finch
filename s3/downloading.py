import boto3
from progress.bar import IncrementalBar


def countdown_s3(func):
    bar = IncrementalBar('Countdown', max=func)

    for item in range(func):
        bar.next()
        pass
    bar.finish()


def download_f(path_to_file, path_to_save):
    session_s3 = boto3.Session(
        aws_access_key_id='q3dEt2FODkSFF3AeM3_w',
        aws_secret_access_key='BnLcTZbcI7g9brLmGpT3JiLhQD0ouUHkkoHikL85',
        region_name='us-east-1')
    client_s3 = session_s3.client('s3', endpoint_url='https://storage.yandexcloud.net')
    Bucket = 'rosstat-storage'
    client_s3.download_file(Bucket, path_to_file, path_to_save, Callback=countdown_s3)


download_f('test/1.csv', '../1.csv')
