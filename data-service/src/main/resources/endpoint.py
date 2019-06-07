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
   Purpose: Discover API endpoints of a cluster.
"""
import requests

CLOUDERA = "CDH"
HORTONWORKS = "HDP"

class Endpoint(object):
    """
    Object that abstracts an API service
    """
    def __init__(self, service_type, url):
        self.type = service_type
        self.url = url

    def gettype(self):
        """
        Get service type of endpoint
        :return: type of end point
        """
        return self.type

    def geturl(self):
        """
        URL to access endpoint API
        :return: URL String
        """
        return self.url


class Platform(object):
    """
    Platform module that discovers endpoint interfaces from management
    node.
    Currently it is tied to Cloudera and in future if we have Hortonworks then it needs
    to be extended to return HDFS and HBASE endpoints
    """
    def __init__(self):
        pass

    def discover(self, properties):
        """
        Disover API endpoints from cluster manager
        :param properties: properties containing defintion.
        :return:
        """
        pass

    @staticmethod
    def factory(distribution):
        """
        Factory method that returns Platform object based on hadoop distribution
        :param distribution - Provider name of hadoop distribution
        :return: Platform object
        """
        if distribution == ("%s" % CLOUDERA):
            raise Error("Cloudera is not supported")
        elif distribution == ("%s" % HORTONWORKS):
            return Hortonworks()
        elif distribution == "Local":
            return Local()
        elif distribution == "kubernetes":
            return Kubernetes()

class Hortonworks(Platform):
    """
    Hortonworks Endpoint object that discovers endpoint of an
    hadoop cluster depending on the distribution
    """
    def _ambari_request(self, ambari, uri):
        hadoop_manager_ip = ambari[0]
        hadoop_manager_username = ambari[1]
        hadoop_manager_password = ambari[2]
        if uri.startswith("http"):
            full_uri = uri
        else:
            full_uri = 'http://%s:8080/api/v1%s' % (hadoop_manager_ip, uri)

        headers = {'X-Requested-By': hadoop_manager_username}
        auth = (hadoop_manager_username, hadoop_manager_password)
        return requests.get(full_uri, auth=auth, headers=headers).json()

    def _component_hosts(self, component_detail):
        host_list = []
        for host_detail in component_detail['host_components']:
            host_list.append(host_detail['HostRoles']['host_name'])
        return host_list

    def _component_host(self, component_detail):
        return self._component_hosts(component_detail)[0]

    def discover(self, properties):
        endpoints = {}

        ambari = (properties['cm_host'], properties['cm_user'], properties['cm_pass'])
        cluster_name = self._ambari_request(ambari, '/clusters')['items'][0]['Clusters']['cluster_name']

        #TODO this should be httpfs - needed for HA and append mode doesn't work with plain webhdfs
        namenode_components = self._ambari_request(ambari, '/clusters/%s/services/%s/components/%s'
                                                   % (cluster_name, "HDFS", "NAMENODE"))
        hosts = self._component_hosts(namenode_components)
        namenodes = ','.join([host+':50070' for host in hosts])
        endpoints['HDFS'] = Endpoint("HDFS", namenodes)

        hbase_components = self._ambari_request(ambari, '/clusters/%s/services/%s/components/%s'
                                                % (cluster_name, "HBASE", "HBASE_MASTER"))
        endpoints['HBASE'] = Endpoint("HBASE", self._component_host(hbase_components))

        return endpoints

class Local(Platform):
    """
    Platform instance used  for testing purpose
    """
    def discover(self, properties):
        endpoints = {"HDFS": Endpoint("HDFS", "192.168.33.10:50070"),
                     'HBASE': Endpoint("HBASE", "192.168.33.10")}
        return endpoints

class Kubernetes(Platform):
    """
    Endpoint definitions for Kubernetes
    """
    def discover(self, properties):
        endpoints = {"HDFS": Endpoint("HDFS", "pnda-hdfs-namenode:50070"),
                     'HBASE': Endpoint("HBASE", "pnda-hbase-master")}
        return endpoints
