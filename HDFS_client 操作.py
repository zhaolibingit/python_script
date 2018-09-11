# -*- coding: utf-8 -*-
# @Time    : 2018/8/21 9:55
# @Author  : zhaolibin
# @Email   : 13021999163@163.com
# @File    : HDFS_client 操作.py
# @Software: PyCharm

from hdfs import *





class HDFS_client(object):
    def _hdfs_client(self ,hdfs_url):
        self.client = Client(hdfs_url, root=None, proxy=None, timeout=100, session=None)
        return self.client

    def get_list(self):
        result = {}
        hdfs_url = 'http://192.168.50.240:50070'
        client = self._hdfs_client(hdfs_url)
        try:
            for list in client.list(hdfs_path='/', status=True):
                result[list[0]] = list[1]['type']
            return result
        except:
            return "hdfs_errors"

a = HDFS_client()
print a.get_list()







#
# import hdfs
# hdfs_url = "http://192.168.50.240:50070"
# client = hdfs.Client(hdfs_url, root=None, proxy=None, timeout=100, session=None)
#
# #列出
# def list(client,hdfs_path='/'):
#     result = {}
#     for list in client.list(hdfs_path,status=True):
#         result[list[0]] = list[1]['type']
#     return result
#
#
#
#
# print list(client,'/tmp')
# #return client.list(hdfs_path,status=True)
#
# #print type(list(client,'/'))
#
# #print client.status('/')