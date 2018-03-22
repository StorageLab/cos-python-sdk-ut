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
test_bucket = "python-v5-test-" + region + "-" + appid
test_object = "test.txt"
special_file_name = "中文" + "→↓←→↖↗↙↘! \"#$%&'()*+,-./0123456789:;<=>@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
conf = CosConfig(
    Region=region,
    Secret_id=SECRET_ID,
    Secret_key=SECRET_KEY
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


def _creat_test_bucket(test_bucket):
    try:
        response = client.create_bucket(
            Bucket=test_bucket,
        )
    except Exception as e:
        if e.get_error_code() == 'BucketAlreadyOwnedByYou':
            print 'BucketAlreadyOwnedByYou'
        else:
            raise e


def setUp():
    print "start test..."
    print "start create bucket " + test_bucket
    _creat_test_bucket(test_bucket)


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
            Key=file_name,
            ResponseCacheControl='private'
        )
        assert etag == get_response['ETag']
        assert 'private' == get_response['Cache-Control']
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


def test_put_object_contains_x_cos_meta():
    """上传带上自定义头部x-cos"""
    response = client.put_object(
        Bucket=test_bucket,
        Body='T'*1024*1024,
        Key=test_object,
        CacheControl='no-cache',
        ContentDisposition='download.txt',
        Metadata={'x-cos-meta-test': 'testtiedu'}
    )
    assert response
    response = client.get_object(
        Bucket=test_bucket,
        Key=test_object,
    )
    assert response['x-cos-meta-test'] == 'testtiedu'


def test_get_object_if_match_true():
    """下载文件if-match成立"""
    response = client.head_object(
        Bucket=test_bucket,
        Key=test_object
    )
    etag = response['Etag']

    response = client.get_object(
        Bucket=test_bucket,
        Key=test_object,
        IfMatch=etag
    )


def test_get_object_if_match_false():
    """下载文件if-match不成立"""
    etag = '"121313131"'
    try:
        response = client.get_object(
            Bucket=test_bucket,
            Key=test_object,
            IfMatch=etag
        )
    except Exception as e:
        assert "PreconditionFailed" == e.get_error_code()


def test_get_object_if_none_match_true():
    """下载文件if-none-match成立"""
    etag = '"121313131"'
    response = client.get_object(
        Bucket=test_bucket,
        Key=test_object,
        IfNoneMatch=etag
    )


def test_get_object_if_none_match_false():
    """下载文件if-none-match不成立"""
    response = client.head_object(
        Bucket=test_bucket,
        Key=test_object
    )

    etag = response['Etag']
    """有bug"""
    try:
        response = client.get_object(
            Bucket=test_bucket,
            Key=test_object,
            IfNoneMatch=etag
        )
    except Exception as e:
        print str(e)


def test_get_object_non_exist():
    """特殊字符文件下载"""
    try:
        response = client.get_object(
            Bucket=test_bucket,
            Key='not_exist.txt'
        )
    except Exception as e:
        assert e.get_error_code() == 'NoSuchKey'


def test_head_object_non_exist():
    """特殊字符文件下载"""
    try:
        response = client.head_object(
            Bucket=test_bucket,
            Key='not_exist.txt'
        )
    except Exception as e:
        assert e.get_error_code() == 'NoSuchResource'


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


def test_head_object_contains_meta_data():
    """head object,object包含x-cos-meta和各种元数据,指定响应头"""
    response = client.put_object(
         Bucket=test_bucket,
         Body='X'*1024,
         Key=test_object,
         Metadata={'x-cos-meta-tiedu': 'dyw'},
         CacheControl='no-cache',
         ContentDisposition='download.txt'
    )
    assert response
    response = client.head_object(
         Bucket=test_bucket,
         Key=test_object
    )
    assert response['x-cos-meta-tiedu'] == 'dyw'
    assert response['Cache-Control'] == 'no-cache'


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


def test_copy_object_diff_bucket():
    """从另外的bucket拷贝object"""
    copy_source = {'Bucket': 'test04-1252448703', 'Key': '/test.txt', 'Region': 'ap-beijing-1'}
    response = client.copy_object(
        Bucket=test_bucket,
        Key='test.txt',
        CopySource=copy_source
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


def test_abort_multipart_upload_not_exist():
    """abort一个不存在的uploadid，返回失败"""
    uploadid = 'adadada12131312121cc'
    try:
        response = client.abort_multipart_upload(
            Bucket=test_bucket,
            Key='multipartfile.txt',
            UploadId=uploadid
        )
    except Exception as e:
        assert e.get_error_code() == 'NoSuchUpload'


def test_create_complete_only_one_part_multipart_upload():
    """创建一个分块上传，上传多个分块，完成分块上传时只指定一个分块"""
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
        UploadId=uploadid,
        MaxParts=1
    )
    lst = response['Part']
    # complete
    response = client.complete_multipart_upload(
        Bucket=test_bucket,
        Key='multipartfile.txt',
        UploadId=uploadid,
        MultipartUpload={'Part': lst}
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


def test_upload_part_copy():
    """创建一个分块上传，上传分块拷贝，列出分块，完成分块上传"""
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

    # upload part copy
    copy_source = {'Bucket': 'test04-1252448703', 'Key': '/test.txt', 'Region': 'ap-beijing-1'}
    response = client.upload_part_copy(
        Bucket=test_bucket,
        Key='multipartfile.txt',
        UploadId=uploadid,
        PartNumber=3,
        CopySource=copy_source,
        CopySourceRange='bytes=0-2'
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


def test_delete_multiple_objects_not_exist():
    """批量删除文件不存在,返回正常"""
    file_id = str(random.randint(0, 1000)) + str(random.randint(0, 1000))
    file_name1 = "tmp" + file_id + "_delete1_not_exist"
    file_name2 = "tmp" + file_id + "_delete2_not_exist"
    objects = {
        "Quite": "false",
        "Object": [
            {
                "Key": file_name1
            },
            {
                "Key": file_name2
            }
        ]
    }
    response = client.delete_objects(
        Bucket=test_bucket,
        Delete=objects
    )


def test_delete_multiple_objects():
    """批量删除文件"""
    file_id = str(random.randint(0, 1000)) + str(random.randint(0, 1000))
    file_name1 = "tmp" + file_id + "_delete1"
    file_name2 = "tmp" + file_id + "_delete2"
    response1 = client.put_object(
        Bucket=test_bucket,
        Key=file_name1,
        Body='A'*1024*1024
    )
    assert response1
    response2 = client.put_object(
        Bucket=test_bucket,
        Key=file_name2,
        Body='B'*1024*1024*2
    )
    assert response2
    objects = {
        "Quite": "false",
        "Object": [
            {
                "Key": file_name1
            },
            {
                "Key": file_name2
            }
        ]
    }
    response = client.delete_objects(
        Bucket=test_bucket,
        Delete=objects
    )
    assert response


def test_create_head_delete_bucket():
    """创建一个bucket,head它是否存在,最后删除一个空bucket"""
    bucket_id = str(random.randint(0, 1000)) + str(random.randint(0, 1000))
    bucket_name = 'buckettest' + bucket_id + '-' + appid
    response = client.create_bucket(
        Bucket=bucket_name,
        ACL='public-read'
    )
    response = client.head_bucket(
        Bucket=bucket_name
    )
    response = client.delete_bucket(
        Bucket=bucket_name
    )


def test_head_bucket_not_exist():
    """head bucket不存在"""
    try:
        response = client.head_bucket(
            Bucket="not-exist-"+test_bucket
        )
    except CosServiceError as e:
        assert e.get_error_code() == 'NoSuchResource'


def test_put_bucket_illegal():
    """bucket名称非法返回错误"""
    try:
        response = client.create_bucket(
            Bucket="123_"+test_bucket
        )
    except CosServiceError as e:
        assert e.get_error_code() == 'InvalidBucketName'


def test_put_bucket_illegal_bad_host():
    """bucket名称以-开始解析host失败"""
    try:
        response = client.create_bucket(
            Bucket="-123_"+test_bucket
        )
    except Exception as e:
        print str(e)


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


def test_list_objects_empty_bucket():
    """列出bucket下的objects为空的情况"""
    bucket = 'empty-' + test_bucket
    response = client.create_bucket(
        Bucket=bucket
    )
    response = client.list_objects(
        Bucket=bucket
    )
    response = client.delete_bucket(
        Bucket=bucket
    )


def test_list_objects_versions():
    """列出bucket下的带版本信息的objects"""
    response = client.list_objects_versions(
        Bucket=test_bucket,
        MaxKeys=50
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


def test_get_bucket_location():
    """获取bucket的地域信息"""
    response = client.get_bucket_location(
        Bucket=test_bucket
    )
    assert response['LocationConstraint'] == region


def test_get_bucket_location_bucket_not_exist():
    """获取bucket的地域信息,bucket不存在"""
    try:
        response = client.get_bucket_location(
            Bucket='not-exist-'+test_bucket
        )
    except Exception as e:
        assert e.get_error_code() == 'NoSuchBucket'


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


def test_put_get_versioning():
    """设置、获取版本控制"""
    # put versioning
    response = client.put_bucket_versioning(
        Bucket=test_bucket,
        Status='Enabled'
    )
    # wait for sync
    # get versioning
    time.sleep(4)
    response = client.get_bucket_versioning(
        Bucket=test_bucket
    )
    assert response['Status'] == 'Enabled'


def test_put_get_delete_replication():
    """设置、获取、删除跨园区复制配置"""
    replication_config = {
        'Role': 'qcs::cam::uin/735905558:uin/735905558',
        'Rule': [
            {
                'ID': '123',
                'Status': 'Enabled',
                'Prefix': 'replication',
                'Destination': {
                    'Bucket': 'qcs:id/0:cos:cn-south:appid/1252448703:replicationsouth'
                }
            }
        ]
    }
    # source dest bucket must enable versioning
    # put replication
    response = client.put_bucket_replication(
        Bucket=test_bucket,
        ReplicationConfiguration=replication_config
    )
    # wait for sync
    # get replication
    time.sleep(4)
    response = client.get_bucket_replication(
        Bucket=test_bucket
    )
    assert response
    # delete lifecycle
    response = client.delete_bucket_replication(
        Bucket=test_bucket
    )


def test_list_multipart_uploads():
    """获取所有正在进行的分块上传"""
    response = client.list_multipart_uploads(
        Bucket=test_bucket,
        Prefix="multipart",
        MaxUploads=100
    )
    # abort make sure delete all uploads
    if 'Upload' in response.keys():
        for data in response['Upload']:
            response = client.abort_multipart_upload(
                Bucket=test_bucket,
                Key=data['Key'],
                UploadId=data['UploadId']
            )
    # create a new upload
    response = client.create_multipart_upload(
        Bucket=test_bucket,
        Key='multipartfile.txt',
    )
    assert response
    uploadid = response['UploadId']
    # list again
    response = client.list_multipart_uploads(
        Bucket=test_bucket,
        Prefix="multipart",
        MaxUploads=100
    )
    assert response['Upload'][0]['Key'] == "multipartfile.txt"
    assert response['Upload'][0]['UploadId'] == uploadid
    # abort again make sure delete all uploads
    for data in response['Upload']:
        response = client.abort_multipart_upload(
            Bucket=test_bucket,
            Key=data['Key'],
            UploadId=data['UploadId']
        )


def test_upload_file_multithreading():
    """根据文件大小自动选择分块大小,多线程并发上传提高上传速度"""
    file_name = "thread_1GB"
    gen_file(file_name, 5)  # set 5MB beacuse travis too slow
    st = time.time()  # 记录开始时间
    response = client.upload_file(
        Bucket=test_bucket,
        Key=file_name,
        LocalFilePath=file_name,
        MAXThread=10,
        CacheControl='no-cache',
        ContentDisposition='download.txt'
    )
    ed = time.time()  # 记录结束时间
    if os.path.exists(file_name):
        os.remove(file_name)
    print ed - st


def test_put_object_copy_source_not_exist():
    """拷贝接口,源文件不存在"""
    copy_source = {'Bucket': 'testtiedu-1252448703', 'Key': '/not_exist123.txt', 'Region': 'ap-guangzhou'}
    try:
        response = client.copy_object(
            Bucket=test_bucket,
            Key='copy_10G.txt',
            CopySource=copy_source
        )
    except Exception as e:
        assert e.get_error_code() == 'NoSuchKey'


def test_copy_file_automatically():
    """根据拷贝源文件的大小自动选择拷贝策略，不同园区,小于5G直接copy_object，大于5G分块拷贝"""
    copy_source = {'Bucket': 'testtiedu-1252448703', 'Key': '/thread_1MB', 'Region': 'ap-guangzhou'}
    response = client.copy(
        Bucket=test_bucket,
        Key='copy_10G.txt',
        CopySource=copy_source,
        MAXThread=10
    )


def test_upload_empty_file():
    """上传一个空文件,不能返回411错误,然后下载这个文件"""
    file_name = "empty.txt"
    with open(file_name, 'wb') as f:
        pass
    with open(file_name, 'rb') as fp:
        response = client.put_object(
            Bucket=test_bucket,
            Body=fp,
            Key=file_name,
            CacheControl='no-cache',
            ContentDisposition='download.txt'
        )
    response = client.get_object(
        Bucket=test_bucket,
        Key=file_name,
        ResponseCacheControl='no-cache',
        ResponseContentDisposition='download.txt'
    )
    response['Body'].get_stream_to_file('download_empty.txt')
    assert response


def test_copy_10G_file_in_same_region():
    """同园区的拷贝,应该直接用copy_object接口,可以直接秒传"""
    copy_source = {'Bucket': test_bucket, 'Key': 'copy_10G.txt', 'Region': region}
    response = client.copy(
        Bucket=test_bucket,
        Key='10G.txt',
        CopySource=copy_source,
        MAXThread=10
    )


def test_use_get_auth():
    """测试利用get_auth方法直接生产签名,然后访问COS"""
    auth = client.get_auth(
        Method='GET',
        Bucket=test_bucket,
        Key='test.txt',
        Params={'acl': '', 'unsed': '123'}
    )
    url = 'http://{bucket}.cos.{region}.myqcloud.com/test.txt?acl&unsed=123'.format(bucket=test_bucket, region=region)
    response = requests.get(url, headers={'Authorization': auth})
    assert response.status_code == 200


if __name__ == "__main__":
    setUp()
    test_upload_empty_file()
    test_put_get_delete_object_10MB()
    test_put_get_versioning()
    test_put_get_delete_replication()
    test_upload_part_copy()
    test_upload_file_multithreading()
    test_copy_file_automatically()
    test_copy_10G_file_in_same_region()
    test_list_objects()
    test_use_get_auth()
    tearDown()
