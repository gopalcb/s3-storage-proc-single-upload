import traceback
import boto3
from role_assume import *

def list_s3_buckets():
    """
    list aws s3 object. return empty list incase any exception
    {
        'Buckets': [
            {
                'Name': 'string',
                'CreationDate': datetime(2015, 1, 1)
            },
        ],
        'Owner': {
            'DisplayName': 'string',
            'ID': 'string'
        }
    }
    """
    try:
        bucket_name_list = []
        s3_client = sts_assume_role()
        response = s3_client.list_buckets()
        for bucket_dict in response['Buckets']:
            bucket_name_list.append(bucket_dict['Name'])

        print(f'INFO: bucket name list: {bucket_name_list}')
        return bucket_name_list
        
    except Exception as e:
        print(f'ERROR: {traceback.format_exc()}')
        return []


def list_s3_bucket_objects(source_bucket):
    """
    resulting objects:
    {
        'Contents': [
            {
                'ETag': '"70ee1738b6b21e2c8a43f3a5ab0eee71"',
                'Key': 'example1.jpg',
                'LastModified': datetime(2014, 11, 21, 19, 40, 5, 4, 325, 0),
                'Owner': {
                    'DisplayName': 'myname',
                    'ID': '12345example25102679df27bb0ae12b3f85be6f290b936c4393484be31bebcc',
                },
                'Size': 11,
                'StorageClass': 'STANDARD',
            },
            {
                'ETag': '"9c8af9a76df052144598c115ef33e511"',
                'Key': 'example2.jpg',
                'LastModified': datetime(2013, 11, 15, 1, 10, 49, 4, 319, 0),
                'Owner': {
                    'DisplayName': 'myname',
                    'ID': '12345example25102679df27bb0ae12b3f85be6f290b936c4393484be31bebcc',
                },
                'Size': 713193,
                'StorageClass': 'STANDARD',
            },
        ],
        ...
    }
    """
    try:
        available_buckets = list_s3_buckets()
        if source_bucket not in available_buckets:
            raise Exception('ERROR: Source bucket does not exists')
            
        s3_client = sts_assume_role()
        paginator = s3_client.get_paginator('list_objects')
        params = {
            'Bucket': source_bucket
        }
        page_iterator = paginator.paginate(**params)
    
        data_object_list = []
        for page in page_iterator:
            data = [content for content in page['Contents']]
            data_object_list = [*data_object_list, *data]
    
        return data_object_list
        
    except Exception as e:
        print(f'ERROR: {traceback.format_exc()}')
        return []


def create_aws_s3_bucket(bucket_name):
    """
    create s3 bucket if not exists
    """
    available_buckets = list_s3_buckets()
    if bucket_name in available_buckets:
        raise Exception('ERROR: Bucket already exists')
        
    s3_client = sts_assume_role()
    s3_client.create_bucket(
        Bucket=bucket_name
    )
    print('INFO: Create s3 bucket - success')
    return True


def delete_aws_s3_bucket(bucket_name):
    """
    delete bucket if exists
    """
    available_buckets = list_s3_buckets()
    if bucket_name in available_buckets:
        raise Exception('ERROR: Bucket does not exist')
        
    s3_client = sts_assume_role()
    bucket = s3_client.Bucket(bucket_name)
    bucket.delete()
    print('INFO: Bucket delete success')
    return True


def upload_data_to_s3_bucket(bucket_name, folder, file_name, data):
    try:
        print(f'Uploading data to {bucket_name} bucket')
        data_string = json.dumps(data, indent=2, default=str)
        
        s3_client = sts_assume_role()
        response = s3_client.put_object(
            Bucket=bucket_name, 
            Key=f'{folder}/{file_name}',
            Body=data_string
        )
        
        code = response['ResponseMetadata']['HTTPStatusCode']
        if code == 200:
            print('INFO: Upload success')
            return True
            
        print('ERROR: Upload failed')
        return False
        
    except Exception as e:
        print(f'ERROR: {traceback.format_exc()}')
        return False


def test_upload_fake_data():
    """
    fake data test upload
    """
    upload_count = 10
    batch_count = 0
    uploaded_files = []

    target_bucket_name = 'test-bucket'
    create_aws_s3_bucket(target_bucket_name)
    
    for itr in range(upload_count):
        batch_count = batch_count + 1
        
        data = [fake.profile() for x in range(10)]
        data = {'data': data}
        upload_data_to_s3_bucket(target_bucket_name, f'raw_batch_{batch_count}', f'raw_{batch_count}_file.json', data)
        uploaded_files.append(f'raw_{batch_count}_file.json')
        
        time.sleep(2)

    print('INFO: Upload done')