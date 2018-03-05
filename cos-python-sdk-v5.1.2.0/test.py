# -*- coding=utf-8
import random
import sys
import time
import hashlib
import os
import requests
from qcloud_cos import CosS3Client
from qcloud_cos import CosConfig
from qcloud_cos import CosServiceError

SECRET_ID = os.environ["SECRET_ID"]
SECRET_KEY = os.environ["SECRET_KEY"]
region = os.environ["REGION"]
appid = os.environ["APPID"]
test_bucket = os.environ["BUCKET"]
test_object = 'test.txt'
special_file_name = "中文" + "→↓←→↖↗↙↘! \"#$%&'()*+,-./0123456789:;<=>@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
conf = CosConfig(
    Appid=appid,
    Region=region,
    Access_id=SECRET_ID,
    Access_key=SECRET_KEY
)
client = CosS3Client(conf)


def get_raw_md5(data):
    m2 = hashlib.md5(data)
    etag = '"' + str(m2.hexdigest()) + '"'
    return etag


def gen_file(path, size):
    _file = open(path, 'w')
    _file.seek(1024*1024*size)
    _file.write('cos')
    _file.close()


def print_error_msg(e):
    print e.get_origin_msg()
    print e.get_digest_msg()
    print e.get_status_code()
    print e.get_error_code()
    print e.get_error_msg()
    print e.get_resource_location()
    print e.get_trace_id()
    print e.get_request_id()


def setUp():
    print "start test..."


def tearDown():
    print "function teardown"


def test_put_get_delete_object_10MB():
    """简单上传下载删除10MB小文件"""
    file_size = 5
    file_id = str(random.randint(0, 1000)) + str(random.randint(0, 1000))
    file_name = "tmp" + file_id + "_" + str(file_size) + "MB"
    gen_file(file_name, 10)
    with open(file_name, 'rb') as f:
        etag = get_raw_md5(f.read())
    try:
        # put object
        with open(file_name, 'rb') as fp:
            put_response = client.put_object(
                Bucket=test_bucket,
                Body=fp,
                Key=file_name,
                CacheControl='no-cache',
                ContentDisposition='download.txt'
            )
        assert etag == put_response['ETag']
        # head object
        head_response = client.get_object(
            Bucket=test_bucket,
            Key=file_name
        )
        assert etag == head_response['ETag']
        # get object
        get_response = client.get_object(
            Bucket=test_bucket,
            Key=file_name
        )
        assert etag == get_response['ETag']
        download_fp = get_response['Body'].get_raw_stream()
        assert download_fp
        # delete object
        delete_response = client.delete_object(
            Bucket=test_bucket,
            Key=file_name
        )
    except CosServiceError as e:
        print_error_msg(e)
    if os.path.exists(file_name):
        os.remove(file_name)


def test_put_object_speacil_names():
    """特殊字符文件上传"""
    response = client.put_object(
        Bucket=test_bucket,
        Body='S'*1024*1024,
        Key=special_file_name,
        CacheControl='no-cache',
        ContentDisposition='download.txt'
    )
    assert response


def test_get_object_special_names():
    """特殊字符文件下载"""
    response = client.get_object(
        Bucket=test_bucket,
        Key=special_file_name
    )
    assert response


def test_delete_object_special_names():
    """特殊字符文件删除"""
    response = client.delete_object(
        Bucket=test_bucket,
        Key=special_file_name
    )


def test_put_object_non_exist_bucket():
    """文件上传至不存在bucket"""
    try:
        response = client.put_object(
            Bucket='test0xx-1252448703',
            Body='T'*1024*1024,
            Key=test_object,
            CacheControl='no-cache',
            ContentDisposition='download.txt'
        )
    except CosServiceError as e:
        print_error_msg(e)
        
def skip_test_copy_object_diff_bucket():
    """从另外的bucket拷贝object"""
    copy_source = {'Bucket': 'test04-1252448703', 'Key': '/test.txt', 'Region': 'ap-beijing-1'}
    response = client.copy_object(
        Bucket=test_bucket,
        Key='test.txt',
        CopySource=copy_source
    )
    assert response

def test_put_object_acl():
    """设置object acl"""
    response = client.put_object_acl(
        Bucket=test_bucket,
        Key=test_object,
        ACL='public-read-write'
    )


def test_get_object_acl():
    """获取object acl"""
    response = client.get_object_acl(
        Bucket=test_bucket,
        Key=test_object
    )
    assert response


def test_create_abort_multipart_upload():
    """创建一个分块上传，然后终止它"""
    # create
    response = client.create_multipart_upload(
        Bucket=test_bucket,
        Key='multipartfile.txt',
    )
    assert response
    uploadid = response['UploadId']
    # abort
    response = client.abort_multipart_upload(
        Bucket=test_bucket,
        Key='multipartfile.txt',
        UploadId=uploadid
    )


def test_create_complete_multipart_upload():
    """创建一个分块上传，上传分块，列出分块，完成分块上传"""
    # create
    response = client.create_multipart_upload(
        Bucket=test_bucket,
        Key='multipartfile.txt',
    )
    uploadid = response['UploadId']
    # upload part
    response = client.upload_part(
        Bucket=test_bucket,
        Key='multipartfile.txt',
        UploadId=uploadid,
        PartNumber=1,
        Body='A'*1024*1024*2
    )

    response = client.upload_part(
        Bucket=test_bucket,
        Key='multipartfile.txt',
        UploadId=uploadid,
        PartNumber=2,
        Body='B'*1024*1024*2
    )
    # list parts
    response = client.list_parts(
        Bucket=test_bucket,
        Key='multipartfile.txt',
        UploadId=uploadid
    )
    lst = response['Part']
    # complete
    response = client.complete_multipart_upload(
        Bucket=test_bucket,
        Key='multipartfile.txt',
        UploadId=uploadid,
        MultipartUpload={'Part': lst}
    )

def test_create_delete_bucket():
    """创建一个bucket,最后删除一个空bucket"""
    bucket_id = str(random.randint(0, 1000)) + str(random.randint(0, 1000))
    bucket_name = 'buckettest' + bucket_id + '-1252448703'
    response = client.create_bucket(
        Bucket=bucket_name,
        ACL='public-read'
    )
    response = client.delete_bucket(
        Bucket=bucket_name
    )


def test_put_bucket_acl_illegal():
    """设置非法的ACL"""
    try:
        response = client.put_bucket_acl(
            Bucket=test_bucket,
            ACL='public-read-writ'
        )
    except CosServiceError as e:
        print_error_msg(e)


def test_get_bucket_acl_normal():
    """正常获取bucket ACL"""
    response = client.get_bucket_acl(
        Bucket=test_bucket
    )
    assert response


def test_list_objects():
    """列出bucket下的objects"""
    response = client.list_objects(
        Bucket=test_bucket,
        MaxKeys=100,
        Prefix='中文',
        Delimiter='/'
    )
    assert response


def test_get_presigned_url():
    """生成预签名的url下载地址"""
    url = client.get_presigned_download_url(
        Bucket=test_bucket,
        Key='中文.txt'
    )
    assert url
    print url


def test_put_get_delete_cors():
    """设置、获取、删除跨域配置"""
    cors_config = {
        'CORSRule': [
            {
                'ID': '1234',
                'AllowedOrigin': ['http://www.qq.com'],
                'AllowedMethod': ['GET', 'PUT'],
                'AllowedHeader': ['x-cos-meta-test'],
                'ExposeHeader': ['x-cos-meta-test1'],
                'MaxAgeSeconds': 500
            }
         ]
    }
    # put cors
    response = client.put_bucket_cors(
        Bucket=test_bucket,
        CORSConfiguration=cors_config
    )
    # wait for sync
    # get cors
    time.sleep(4)
    response = client.get_bucket_cors(
        Bucket=test_bucket
    )
    assert response
    # delete cors
    response = client.get_bucket_cors(
        Bucket=test_bucket
    )


def test_put_get_delete_lifecycle():
    """设置、获取、删除生命周期配置"""
    lifecycle_config = {
        'Rule': [
            {
                'Expiration': {'Days': 100},
                'ID': '123',
                'Filter': {'Prefix': ''},
                'Status': 'Enabled',
            }
        ]
    }
    # put lifecycle
    response = client.put_bucket_lifecycle(
        Bucket=test_bucket,
        LifecycleConfiguration=lifecycle_config
    )
    # wait for sync
    # get lifecycle
    time.sleep(4)
    response = client.get_bucket_lifecycle(
        Bucket=test_bucket
    )
    assert response
    # delete lifecycle
    response = client.delete_bucket_lifecycle(
        Bucket=test_bucket
    )


if __name__ == "__main__":
    setUp()
    tearDown()
