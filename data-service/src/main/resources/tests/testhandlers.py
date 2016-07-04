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
   Purpose: API Handler tests
"""

import json
import logging
import unittest

from tornado.httputil import HTTPHeaders
from tornado.testing import AsyncHTTPTestCase
from tornado_json.application import Application
from tornado_json.routes import get_routes


from db import TestDB
from main.resources import dataservice

# Disable tornado access warnings


logging.getLogger("tornado.access").propagate = False
logging.getLogger("tornado.access").addHandler(logging.NullHandler())


class TestServer(AsyncHTTPTestCase):
    def setUp(self):
        super(TestServer, self).setUp()
        name = str(self).split(" ")
        self.name = name[0].replace("_", "") + name[1].split(".")[-1][:-1]


    def get_app(self):
        routes = get_routes(dataservice)
        print routes
        settings = dict(
            hbase_host='192.168.33.10',
            hbase_thrift_port=9095,
            hdfs_host='192.168.33.10'
        )
        self.db = TestDB()
        return Application(routes=routes, settings=settings, db_conn=self.db)

    def tearDown(self):
        super(TestServer, self).tearDown()


class ListHandler(TestServer):
    def test_list_api(self):
        result = self.fetch("/api/v1/datasets", method="GET")
        print result.body
        self.assertEqual(result.code, 200)


class UpdateHandler(TestServer):
    def test_get_dataset(self):
        result = self.fetch("/api/v1/datasets/test3", method="GET")
        print result.body
        self.assertEqual(result.code, 200)

    def test_get_dataset_not_exist(self):
        result = self.fetch("/api/v1/datasets/redbull", method="GET")
        print result.body
        self.assertEqual(result.code, 500)

    def test_put_dataset_for_create(self):
        request_data = dict(mode='delete')
        result = self.fetch("/api/v1/datasets/redbull", method="PUT",body=json.dumps(request_data),
                            headers=HTTPHeaders({"content-type": "application/json"}))
        self.assertEqual(result.code, 400)
        request_data = dict(id= 'test', policy='age', path='/test', mode = 'archive')
        result = self.fetch("/api/v1/datasets/redbull", method="PUT", body=json.dumps(request_data),
                            headers=HTTPHeaders({"content-type": "application/json"}))
        self.assertEqual(result.code, 400)
        request_data["max_age_days"] = 20
        result = self.fetch("/api/v1/datasets/redbull", method="PUT", body=json.dumps(request_data),
                            headers=HTTPHeaders({"content-type": "application/json"}))
        self.assertEqual(result.code, 200)
        request_data["max_size_gigabytes"] = 20
        result = self.fetch("/api/v1/datasets/redbull", method="PUT", body=json.dumps(request_data),
                            headers=HTTPHeaders({"content-type": "application/json"}))
        self.assertEqual(result.code, 200)

    def test_put_dataset_for_invalid_age(self):
        data = dict(policy="age", retention="2000")
        print json.dumps(data)
        result = self.fetch("/api/v1/datasets/test3", method="PUT", body=json.dumps(data),
                            headers=HTTPHeaders({"content-type": "application/json"}))
        self.assertEqual(result.code, 400)

    def test_put_dataset_for_valid_age(self):
        data = dict(policy="age", max_age_days=20)
        result = self.fetch("/api/v1/datasets/test3", method="PUT", body=json.dumps(data),
                            headers=HTTPHeaders({"content-type": "application/json"}))
        # self.assertEqual(self.db.data['cf:retention'], str(data['max_age_days']))
        self.assertEqual(result.code, 200)

    def test_put_dataset_for_invalid_size(self):
        data = dict(policy="size", max_age_days=20)
        result = self.fetch("/api/v1/datasets/test3", method="PUT", body=json.dumps(data),
                            headers=HTTPHeaders({"content-type": "application/json"}))
        self.assertEqual(result.code, 400)

    def test_put_dataset_for_size(self):
        data = dict(policy="size", max_size_gigabytes=20)
        result = self.fetch("/api/v1/datasets/test3", method="PUT", body=json.dumps(data),
                            headers=HTTPHeaders({"content-type": "application/json"}))
        self.assertEqual(result.code, 200)

    def test_put_dataset_for_mode(self):
        request_data = dict(mode='archive')
        result = self.fetch("/api/v1/datasets/test3", method="PUT", body=json.dumps(request_data),
                            headers=HTTPHeaders({"content-type": "application/json"}))
        self.assertEqual(result.code, 200)

        request_data = dict(mode='keep')
        result = self.fetch("/api/v1/datasets/test3", method="PUT", body=json.dumps(request_data),
                            headers=HTTPHeaders({"content-type": "application/json"}))
        self.assertEqual(result.code, 200)

        request_data = dict(mode='delete')
        result = self.fetch("/api/v1/datasets/test3", method="PUT", body=json.dumps(request_data),
                            headers=HTTPHeaders({"content-type": "application/json"}))
        self.assertEqual(result.code, 200)
        request_data = dict(mode='keep')
        result = self.fetch("/api/v1/datasets/test3", method="PUT", body=json.dumps(request_data),
                            headers=HTTPHeaders({"content-type": "application/json"}))
        self.assertEqual(result.code, 200)
        request_data = dict(mode='invalid')
        result = self.fetch("/api/v1/datasets/test3", method="PUT", body=json.dumps(request_data),
                            headers=HTTPHeaders({"content-type": "application/json"}))
        self.assertNotEqual(result.code, 200)

if __name__ == "__main__":
    unittest.main()
