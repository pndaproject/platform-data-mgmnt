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
   Purpose: Datstore that manages datasets and its policy through persistence layer(HBase)
"""


import logging
import os
import re


import happybase
from pyhdfs import HdfsClient, HdfsException
from thrift.Thrift import TException

from .dbenum import DATASET
from .dbenum import DBSCHEMA
from .dbenum import POLICY

DB_CONNECTION_POOL_SIZE = 8
DB_CONNECTION_TIME_OUT = 5000
KITE_COMMAND = 'kite-api'


def onerror(msg):
    """
    Callback invoked by HDFS module when there are errors
    """
    print " Error in HDFS Walk {}".format(msg)


def tag_for_integrity(data_list):
    """
    Tag datasets for integrity error
    :param data_list:
    :return:
    """
    if len(data_list) > 0:
        for i in data_list:
            i['policy'] = DATASET.INTEGRITY_ERROR
    return data_list


def dirwalk(client, dir_path):
    """
    Function to walk hdfs DIRECTORY

    """
    for file_entry in client.listdir(dir_path):
        fullpath = os.path.join(dir_path, file_entry)
        file_type = client.get_file_status(fullpath).type
        if file_type == 'DIRECTORY':
            for entry in dirwalk(client, fullpath):  # recurse into subdir
                yield entry
        else:
            yield dir_path


class Singleton(type):
    """
    Singleton using metaclass
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args)
        return cls._instances[cls]


class HDBDataStore(object):
    """
    Singleton class to read and maintain datasets for Service API
    Its not a generic HBase dataset handler.
    """
    __metaclass__ = Singleton
    def __init__(self, hdfs_host, hbase_host, hbase_port_no, table_name, repo_path):
        logging.info(
            'Open connection pool for hbase host:%s port:%d', hbase_host, hbase_port_no)
        # create connection pools
        try:
            self.conn_pool = happybase.ConnectionPool(DB_CONNECTION_POOL_SIZE, host=hbase_host,
                                                      port=hbase_port_no,
                                                      timeout=DB_CONNECTION_TIME_OUT)
        except TException as exception:
            logging.warn("Exception throw for HBase Connection pool creation{%s}",
                         exception.message)
        self.hbase_host = hbase_host
        self.hdfs_host = hdfs_host
        self.hbase_port_no = hbase_port_no
        self.table_name = table_name
        self.repo_path = repo_path
        self.master_dataset = list()
        self.client = HdfsClient(hosts=hdfs_host, user_name='hdfs')

    def collect(self):
        """
        Collect datasets by reading from HDFS Repo and HBase repo
        :return:
        """
        hdfs_list = self.read_data_from_repo()
        hbase_list = self.retrieve_datasets_from_hbase()
        inter_list = list()
        # find intersection and keep hbase copy
        for hbase_entry, hdfs_entry in [(hbase_entry, hdfs_entry) for hbase_entry in hbase_list
                                        for hdfs_entry in hdfs_list]:
            if hbase_entry['id'] == hdfs_entry['id']:
                # remove entries in HDFS list that matches hbase
                inter_list.append(hbase_entry)
                hdfs_list.remove(hdfs_entry)
                hbase_list.remove(hbase_entry)
        # yes intersection
        if len(inter_list) > 0:
            logging.debug("The intersection list:%s is", inter_list)
            self.master_dataset = inter_list + hdfs_list
            if len(hbase_list) != 0:
                logging.warn(" Warning Untracked datasets of size %d", len(hbase_list))
                self.master_dataset = self.master_dataset + tag_for_integrity(hbase_list)
        else:
            # god knows whats happening
            self.master_dataset = tag_for_integrity(hbase_list) + hdfs_list

    def read_data_from_repo(self):
        """
        Read data from HDFS repo_path
        :return:
        """
        repo_path = self.repo_path
        hdfs_dataset = list()
        try:
            for root, dirs, _ in self.client.walk(repo_path, topdown=True, onerror=onerror):
                for entry in dirs:
                    if "source=" in entry:
                        item = {DATASET.ID: re.sub('source=', '', entry),
                                DATASET.POLICY: POLICY.SIZE,
                                DATASET.PATH: os.path.join(root, entry), DATASET.MODE: 'keep'}
                        hdfs_dataset.append(item)
                break
        except HdfsException as exception:
            logging.warn("Error in walking HDFS File system %s", exception.message)
        return hdfs_dataset

    def retrieve_datasets_from_hbase(self):
        """
        Connect to hbase table and return list of hbase_dataset
        :return:
        """
        hbase_datasets = list()
        table_name = self.table_name
        try:
            with self.conn_pool.connection(DB_CONNECTION_TIME_OUT) as connection:
                if connection.is_table_enabled(table_name):
                    table = connection.table(table_name)
                else:
                    logging.info('creating hbase table %s', table_name)
                    connection.create_table(table_name, {'cf': dict()})
                    table = connection.table(table_name)
                for _, data in table.scan(limit=1):
                    logging.debug('%s found', table_name)
        except TException as exception:
            logging.warn(" failed to read table from hbase error(%s):", exception.message)
            return hbase_datasets
        logging.debug('connecting to hbase to read hbase_dataset')
        for key, data in table.scan():
            item = {DATASET.ID: key, DATASET.PATH: data[DBSCHEMA.PATH],
                    DATASET.POLICY: data[DBSCHEMA.POLICY],
                    DATASET.MODE: data[DBSCHEMA.MODE]}
            if item[DATASET.POLICY] == POLICY.AGE:
                item[DATASET.MAX_AGE] = int(data[DBSCHEMA.RETENTION])
            elif item[DATASET.POLICY] == POLICY.SIZE:
                item[DATASET.MAX_SIZE] = int(data[DBSCHEMA.RETENTION])
            hbase_datasets.append(item)
        logging.info(hbase_datasets)
        return hbase_datasets

    def read_datasets(self):
        """
        Connect to hbase table and return list of datasets
        :return:
        """
        return self.master_dataset

    def read_partitions(self, data_path):
        """
        Read partition for a HDFS dataset
        :param data_path:
        :return:
        """
        data_parts = list()
        try:
            for entry in dirwalk(self.client, data_path):
                if entry not in data_parts:
                    data_parts.append(entry)
        except HdfsException as exception:
            logging.warn(
                "Error in walking HDFS File system for partitions errormsg:%s", exception.message)
        return data_parts

    def write_dataset(self, data):
        """
        Persist dataset entry into HBase Table
        :param data: api that needs update
        :return: None
        """
        try:
            logging.debug("Write dataset:{%s}", data)
            table_name = self.table_name
            with self.conn_pool.connection(DB_CONNECTION_TIME_OUT) as connection:
                table = connection.table(table_name)
                dataset = {DBSCHEMA.PATH: data[DATASET.PATH], DBSCHEMA.POLICY: data[DATASET.POLICY],
                           DBSCHEMA.MODE: data[DATASET.MODE]}
                if DATASET.RETENTION in data:
                    dataset[DBSCHEMA.RETENTION] = data[DATASET.RETENTION]
                logging.debug("calling put on table for %s", dataset)
                table.put(data[DATASET.ID], dataset)
        except TException as exception:
            logging.warn("Failed to write dataset into hbase,  error(%s):", exception.message)

    def delete_dataset(self, data):
        """
        Delete dataset entry from HBase.
        :param data: dataset instance
        :return: None
        """
        try:
            table_name = self.table_name
            with self.conn_pool.connection(DB_CONNECTION_TIME_OUT) as connection:
                table = connection.table(table_name)
                logging.debug("Deleting dataset from HBase:{%s}", data)
                table.delete(data['id'])
        except TException as exception:
            logging.warn("Failed to delete dataset in hbase,  error(%s):", exception.message)


