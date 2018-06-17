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
   Purpose: Defines configuration option for running Data API service.
"""


from tornado.options import define

CONFIG_FILENAME = "server.conf"


def define_options():
    """
    Define default configurations for data service
    :return:
    """
    define("conf_file_path", default=CONFIG_FILENAME, help="Path for configuration file", type=str)
    define("ports", default=[8000],
           help="A list of ports that will be tried unless one can be bound to", type=list)
    define("bind_address", default='0.0.0.0', help="The address server will be bound to", type=str)
    define("sync_period", default=5000, help="Time interval in which the service will sync data",
           type=int)
    define("datasets_table", default='platform_datasets',
           help="The hbase table in which data repo info are maintained",
           type=str)
    define("data_repo", default="/user/PNDA/datasets",
           help="The HDFS location in which all HDFS files are stored",
           type=str)
    define("thrift_port", default=9090, help="The port number of HBASE Thrift gateway", type=int)
    define("hadoop_distro", default='CDH', help="The hadoop distribution (CDH|HDP)", type=str)
    define("cm_host", default='localhost', help="The cluster manager interface", type=str)
    define("cm_user", default='admin', help="The user name for cluster manager", type=str)
    define("cm_pass", default='admin', help="The password for cluster manager", type=str)
