import boto3
import os
import json


def upload_files():
    with open('aws_config.json', 'r') as f_aws:
        aws_config = json.load(f_aws)
    aws_access_key = aws_config['aws_access_key_id']
    aws_secret_key = aws_config['aws_secret_access_key']

    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    lst_files = os.listdir('results/summary_stats/')
    for f in lst_files:
        s3.meta.client.upload_file('results/summary_stats/' + f, 'bucketname', 'summary_stats/' + f)

if __name__ == '__main__':
    upload_files()

