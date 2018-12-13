# -*- coding:utf-8 -*-

from __future__ import unicode_literals

import sys

# sys.path.append('/home/anyi/anaconda3/lib/')

sys.path.append('/home/anyi/anaconda3/envs/hdfs_demo1/lib/')
from hdfs3 import HDFileSystem

test_host = 'localhost'
test_port = 9000


def hdfs_exists(hdfs_client):
    path = '/test/test/'
    if hdfs_client.exists(path):
        hdfs_client.rm(path)

    hdfs_client.makedirs(path)


def hdfs_write_read(hdfs_client):
    data = b'hello hadoop' * 20
    file_a = '/test/test/file_a'
    with hdfs_client.open(file_a, 'wb', replication=1) as f:
        f.write(data)

    with hdfs_client.open(file_a, 'rb') as f:
        out = f.read(len(data))
        
        assert out == data


def hdfs_readlines(hdfs_client):
    file_b = '/test/test/file_b'
    with hdfs_client.open(file_b, 'wb', replication=1) as f:
        f.write(b'hello\nhadoop')

    with hdfs_client.open(file_b, 'rb') as f:
        lines = f.readlines()
        print(lines)
        assert len(lines) == 2


if __name__ == '__main__':
    hdfs_client = HDFileSystem(host=test_host, port=test_port)

    hdfs_exists(hdfs_client)

    hdfs_write_read(hdfs_client)

    hdfs_readlines(hdfs_client)

    hdfs_client.disconnect()
    
    print('-' * 20)
    print('hello hadoop')
