"""
     Copyright (c) 2016 Cisco and/or its affiliates.
     This software is licensed to you under the terms of the Apache License, Version 2.0
     (the "License").
     You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
     The code, technical concepts, and all information contained herein, are the property of
     Cisco Technology, Inc.and/or its affiliated entities, under various laws including copyright,
     international treaties, patent, and/or contract.
     Any use of the material herein must be in accordance with the terms of the License.
     All rights not expressly granted by the License are reserved.
     Unless required by applicable law or agreed to separately in writing, software distributed
     under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
     ANY KIND, either express or implied.
     Purpose: Tests for hdb
"""

from unittest import TestCase


import mock as mock
from mock import Mock
from mock import MagicMock
from ..dataservice import HDBDataStore


def get_repo_samples1():
    item1 = {"id": 'test', 'policy': 'keep', 'path': 'repo', 'retention': '222'}
    item2 = {"id": 'test2', 'policy': 'keep', 'path': 'repo', 'retention': '222'}
    item3 = {"id": 'test3', 'policy': 'keep', 'path': 'repo', 'retention': '222'}
    items = list()
    items.append(item1)
    items.append(item2)
    items.append(item3)
    return items


def get_repo_samples2(error):
    if error is True:
        item1 = {"id": 'sample1', 'policy': 'integrity_error', 'path': 'repo', 'retention': '222'}
        item2 = {"id": 'sample2', 'policy': 'integrity_error', 'path': 'repo', 'retention': '222'}
        item3 = {"id": 'sample3', 'policy': 'integrity_error', 'path': 'repo', 'retention': '222'}
    else:
        item1 = {"id": 'sample1', 'policy': 'age', 'path': 'repo', 'retention': '222'}
        item2 = {"id": 'sample2', 'policy': 'age', 'path': 'repo', 'retention': '222'}
        item3 = {"id": 'sample3', 'policy': 'age', 'path': 'repo', 'retention': '222'}
    items = list()
    items.append(item1)
    items.append(item2)
    items.append(item3)
    return items


def get_repo_sample3():
    item1 = {"id": 'test', 'policy': 'age', 'path': 'repo', 'retention': '222'}
    item2 = {"id": 'test2', 'policy': 'size', 'path': 'repo', 'retention': '222'}
    item3 = {"id": 'test5', 'policy': 'keep', 'path': 'repo', 'retention': '222'}
    items = list()
    items.append(item1)
    items.append(item2)
    items.append(item3)
    return items


class TestHDB(TestCase):
    def get_hdb(self):
        hbase_host = '192.168.33.10'
        hbase_thrift_port = 9095
        db1 = HDBDataStore(hbase_host, hbase_host, hbase_thrift_port, "platform_datasets",
                           "repo:hdfs://192.168.33.10:/user/pnda/")
        return db1

    @mock.patch('happybase.ConnectionPool')
    def test_singleton(self, hbase):
        # pylint: disable=unused-argument
        db1 = self.get_hdb()
        db2 = self.get_hdb()
        self.assertEquals(db1, db2)

    @mock.patch('happybase.ConnectionPool')
    def test_collect_same_values(self, hbase):
        # pylint: disable=unused-argument
        db1 = self.get_hdb()
        db1.read_data_from_repo = Mock(return_value=get_repo_samples1())
        db1.retrieve_datasets_from_hbase = Mock(return_value=get_repo_samples1())
        db1.collect()
        self.assertEqual(db1.read_datasets(), get_repo_samples1())

    @mock.patch('happybase.ConnectionPool')
    def test_collect_no_intersection(self, hbase):
        # pylint: disable=unused-argument
        db1 = self.get_hdb()
        db1.read_data_from_repo = Mock(return_value=get_repo_samples1())
        db1.retrieve_datasets_from_hbase = Mock(return_value=get_repo_samples2(False))
        db1.collect()
        self.assertEqual(db1.read_datasets(), get_repo_samples2(True) + get_repo_samples1())

    @mock.patch('happybase.ConnectionPool')
    def test_collect_intersection(self, hbase):
        # pylint: disable=unused-argument
        db1 = self.get_hdb()
        db1.read_data_from_repo = Mock(return_value=get_repo_samples1())
        db1.retrieve_datasets_from_hbase = Mock(return_value=get_repo_sample3())
        db1.collect()
        datasets = db1.read_datasets()
        for i in datasets:
            if i['id'] == 'test5':
                self.assertEqual(i['policy'], 'integrity_error')
            elif i['id'] == 'test3':
                self.assertEqual(i['policy'], 'keep')

    @mock.patch('happybase.ConnectionPool')
    def test_callback_emptylist_repo(self, hbase):
        # pylint: disable=unused-argument
        db1 = self.get_hdb()
        db1.read_data_from_repo = Mock(return_value=list())
        db1.retrieve_datasets_from_hbase = Mock(return_value=get_repo_samples2(False))
        db1.collect()
        self.assertEqual(db1.read_datasets(), get_repo_samples2(error=True) + list())

    @mock.patch('happybase.ConnectionPool')
    def test_callback_emptylist_hbase(self, hbase):
        # pylint: disable=unused-argument
        db1 = self.get_hdb()
        db1.read_data_from_repo = Mock(return_value=get_repo_samples1())
        db1.retrieve_datasets_from_hbase = Mock(return_value=list())
        db1.collect()
        self.assertEqual(db1.read_datasets(), get_repo_samples1() + list())

    @mock.patch('happybase.ConnectionPool')
    @mock.patch('subprocess.Popen')
    # Incomplete
    def test_read_from_repo(self, hbase, p_open):
        # pylint: disable=unused-argument
        db1 = self.get_hdb()

        def values():
            items = ["api:hdfs://192.168.33.10/user/pnda/default/sandwich2",
                     "api:hdfs://192.168.33.10/user/pnda/default/sandwich1"]
            return items

        process_mock = Mock()
        attrs = {'communicate.return_value': (''.join(values()), 'error')}
        process_mock.configure_mock(**attrs)
        p_open.return_value = process_mock
        db1.read_data_from_repo()
        # self.assertTrue(process_mock.called)

    def test_write_dataset(self):
        hbase_host = '192.168.33.10'
        hbase_thrift_port = 9095
        table = MagicMock()
        connection = MagicMock()
        enter = MagicMock()
        enter.table.return_value = table
        connection.__enter__.return_value = enter
        db1 = HDBDataStore(hbase_host, hbase_host, hbase_thrift_port, "platform_datasets",
                           "repo:hdfs://192.168.33.10:/user/pnda/")
        db1.conn_pool = MagicMock(name="ConnectionPool")
        db1.conn_pool.connection.return_value = connection
        sample_data = {"id": 'test', 'policy': 'age', 'path': 'repo', 'retention': '222',
                       'mode':"archive"}
        db1.write_dataset(sample_data)
        table.put.assert_called_once_with('test', {'cf:mode': 'archive', 'cf:policy': 'age',
                                                   'cf:path': 'repo', 'cf:retention': '222'})
