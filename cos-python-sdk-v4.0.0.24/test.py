#!/usr/bin/env python
# -*- coding: utf-8 -*-

from qcloud_cos import CosClient
from qcloud_cos import UploadFileRequest
from qcloud_cos import UploadFileFromBufferRequest
from qcloud_cos import UpdateFileRequest
from qcloud_cos import UpdateFolderRequest
from qcloud_cos import DelFileRequest
from qcloud_cos import DelFolderRequest
from qcloud_cos import CreateFolderRequest
from qcloud_cos import StatFileRequest
from qcloud_cos import StatFolderRequest
from qcloud_cos import ListFolderRequest
from qcloud_cos import DownloadFileRequest
from qcloud_cos import DownloadObjectRequest
from qcloud_cos import MoveFileRequest

import logging
import sys
import os
logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
logger = logging.getLogger(__name__)


secret_id = os.environ["SECRET_ID"]
secret_key = os.environ["SECRET_KEY"]
appid = 1252448703
region = 'guangzhou'
bucket = u'tieduv4'
cos_client = CosClient(appid, secret_id, secret_key, region)

def setUp():
    print "start test..."

def tearDown():
    print "function teardown"

def test_upload_file_default():
    """上传默认不覆盖"""
    with open('local_file_1.txt', 'w') as f:
        f.write("hello world1")
    request = UploadFileRequest(bucket, u'/sample_file.txt', u'local_file_1.txt')
    upload_file_ret = cos_client.upload_file(request)
    assert upload_file_ret['code'] == 0

def test_upload_file_insert_only():
    """上传设置insert_only为0覆盖"""
    with open('local_file_2.txt', 'w') as f:
        f.write("hello world2")
    request = UploadFileRequest(bucket, u'/sample_file.txt', u'local_file_2.txt')
    request.set_insert_only(0)  # 设置允许覆盖
    upload_file_ret = cos_client.upload_file(request)
    assert upload_file_ret['code'] == 0

def test_upload_file_from_buffer_insert_only():
    """从内存上传文件"""
    data = "i am from buffer"
    request = UploadFileFromBufferRequest(bucket, u'/sample_file.txt', data)
    request.set_insert_only(0)  # 设置允许覆盖
    upload_file_ret = cos_client.upload_file_from_buffer(request)
    assert upload_file_ret['code'] == 0

def test_uploda_file_verify_sha1():
    """上传文件验证sha1"""
    request = UploadFileRequest(bucket, u'/sample_file.txt', u'local_file_2.txt')
    request.set_insert_only(0)  # 设置允许覆盖
    request.set_verify_sha1(True)
    upload_file_ret = cos_client.upload_file(request)
    assert upload_file_ret['code'] == 0

def test_download_file_local():
    """下载文件到本地"""
    request = DownloadFileRequest(bucket, u'/sample_file.txt', u'local_file_3.txt')
    download_file_ret = cos_client.download_file(request)
    assert download_file_ret['code'] == 0

def test_download_object():
    """下载文件到内存"""
    request = DownloadObjectRequest(bucket, u'/sample_file.txt')
    fp = cos_client.download_object(request)
    data = fp.read()
    assert data

def test_get_obj_attr():
    """获取文件属性"""
    request = StatFileRequest(bucket, u'/sample_file.txt')
    stat_file_ret = cos_client.stat_file(request)
    assert stat_file_ret['code'] == 0

def test_update_obj_attr():
    """更新文件属性"""
    request = UpdateFileRequest(bucket, u'/sample_file.txt')

    request.set_biz_attr(u'this is demo')             # 设置文件biz_attr属性
    request.set_authority(u'eWRPrivate')              # 设置文件的权限
    request.set_cache_control(u'cache_xxx')           # 设置Cache-Control
    request.set_content_type(u'application/text')     # 设置Content-Type
    request.set_content_disposition(u'ccccxxx.txt')   # 设置Content-Disposition
    request.set_content_language(u'english')          # 设置Content-Language
    request.set_x_cos_meta(u'x-cos-meta-xxx', u'xxx')  # 设置自定义的x-cos-meta-属性
    request.set_x_cos_meta(u'x-cos-meta-yyy', u'yyy')  # 设置自定义的x-cos-meta-属性

    update_file_ret = cos_client.update_file(request)
    assert update_file_ret['code'] == 0

def test_move_file():
    """移动文件"""
    request = MoveFileRequest(bucket, u'/sample_file.txt', u'/sample_file_move.txt')
    move_ret = cos_client.move_file(request)
    assert move_ret['code'] == 0

def test_create_folder():
    """生成目录, 目录名为sample_folder"""
    request = CreateFolderRequest(bucket, u'/sample_folder/')
    create_folder_ret = cos_client.create_folder(request)
    assert create_folder_ret['code'] == 0

def test_update_folder_biz_attr():
    """更新目录的biz_attr属性"""
    request = UpdateFolderRequest(bucket, u'/sample_folder/', u'this is a test folder')
    update_folder_ret = cos_client.update_folder(request)
    assert update_folder_ret['code'] == 0

def test_get_folder_biz_attr():
    """获取目录的属性"""
    request = StatFolderRequest(bucket, u'/sample_folder/')
    stat_folder_ret = cos_client.stat_folder(request)
    assert stat_folder_ret['code'] == 0

def test_list_folder():
    """list目录, 获取目录下的成员"""
    request = ListFolderRequest(bucket, u'/sample_folder/')
    list_folder_ret = cos_client.list_folder(request)
    assert list_folder_ret['code'] == 0

def test_list_folder_use_delimiter():
    """list目录, 使用delimiter"""
    request = ListFolderRequest(bucket, u'/sample_folder/')
    request.set_prefix(u'test')
    request.set_delimiter(u'/')
    list_folder_ret = cos_client.list_folder(request)
    print list_folder_ret
    assert list_folder_ret['code'] == 0

def test_delete_folder():
    """删除目录"""
    request = DelFolderRequest(bucket, u'/sample_folder/')
    delete_folder_ret = cos_client.del_folder(request)
    assert delete_folder_ret['code'] == 0

def test_delete_file():
    """删除文件"""
    request = DelFileRequest(bucket, u'/sample_file_move.txt')
    del_ret = cos_client.del_file(request)
    assert del_ret['code'] == 0

def test_upload_file_chinese():
    """上传中文文件"""
    with open('local_file_1.txt', 'w') as f:
        f.write("hello world1")
    request = UploadFileRequest(bucket, u'/中文.txt', u'local_file_1.txt')
    upload_file_ret = cos_client.upload_file(request)
    assert upload_file_ret['code'] == 0
    
def test_download_file_chinese():
    """下载中文文件"""
    request = DownloadFileRequest(bucket, u'/中文.txt', u'local_file_3.txt')
    download_file_ret = cos_client.download_file(request)
    assert download_file_ret['code'] == 0


if __name__ == '__main__':
    test_upload_file_default()
    pass
