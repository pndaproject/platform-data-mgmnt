"""
     Copyright (c) 2016 Cisco and/or its affiliates.
     This software is licensed to you under the terms of the Apache License,
     Version 2.0 (the "License").

     You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
     The code, technical concepts, and all information contained herein, are the property of
     Cisco Technology, Inc.and/or its affiliated entities, under various laws
     including copyright, international treaties, patent, and/or contract.

     Any use of the material herein must be in accordance with the terms of the License.
     All rights not expressly granted by the License are reserved.

     Unless required by applicable law or agreed to separately in writing,software distributed
     under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
     OF ANY KIND, either express or implied.

     Purpose: API Implementation for managing PNDA platform datasets
"""

import copy
import logging

import jsonschema
from concurrent.futures import ThreadPoolExecutor
from tornado import escape
from tornado.concurrent import run_on_executor
from tornado.gen import Return
from tornado.ioloop import IOLoop
from tornado_json import schema
from tornado_json.exceptions import APIError
from tornado_json.gen import coroutine
from tornado_json.requesthandlers import APIHandler

from ..dbenum import DATASET
from ..dbenum import POLICY

API_VERSION = "v1"

MODE_ENUM_LIST = ["keep", "archive", "delete", DATASET.INTEGRITY_ERROR]
POLICY_ENUM_LIST = [POLICY.AGE, POLICY.SIZE]

DATASET_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "path": {"type": "string"},
        "policy": {"enum": POLICY_ENUM_LIST},
        "mode": {"enum": MODE_ENUM_LIST},
        "max_age_days": {"type": "number"},
        "max_size_gigabytes": {"type": "number"}
    },
    "required": ["id", "path", "policy", "mode"]
}


def remove_keys_from_dict(dict_object, keys):
    """
    Remove specific keys from dict if it exists
    :param dict_object: dictionary object
    :param keys:  key list that needs removal
    :return: dictionary
    """
    for key in keys:
        if key in dict_object:
            del dict_object[key]
    return dict_object


class DataHandler(APIHandler):
    """
    Abstract data handler class
    """
    __url_names__ = [""]
    io_loop = IOLoop.current()
    executor = ThreadPoolExecutor(max_workers=4)

    def data_received(self, chunk):
        pass

    @run_on_executor
    def __read_data__(self):
        hdb_datasets = self.db_conn.read_datasets()
        logging.info("Following datasets were received from table %s", hdb_datasets)
        raise Return(hdb_datasets)

    @run_on_executor
    def __read_parts__(self, path):
        logging.info("Reading partition information for dataset: %s", path)
        parts = self.db_conn.read_partitions(path)
        raise Return(parts)

    @run_on_executor
    def __write_data__(self, data):
        self.db_conn.write_dataset(data)

    @coroutine
    def __get_datasets__(self):
        try:
            yield self.__read_data__()
        except Return as value_return:
            raise Return(value_return.value)

    @coroutine
    def __get_parts__(self, path):
        try:
            yield self.__read_parts__(path)
        except Return as value_return:
            raise Return(value_return.value)


class ListDatasets(DataHandler):
    """
    List available pnda datasets
    """

    __urls__ = [r'/api/' + API_VERSION + '/datasets']

    @schema.validate(
        output_schema={
            "type": "array",
        },
    )
    @coroutine
    def get(self, *args, **kwargs):
        # pylint: disable=unused-argument
        try:
            result = yield self.__get_datasets__()
            if result is None:
                raise APIError(503, log_message="Server internal error")
            # reformat response depending on policy
            raise Return(result)
        except Return as return_value:
            raise return_value
        except Exception as exception:
            logging.warn("Exception thrown in /list API %s", exception.message)
            raise APIError(500, log_message="Server Internal error")


class GetPartitions(DataHandler):
    """
    Return partitions pertaining to a dataset
    """
    __urls__ = [r'/api/' + API_VERSION + '/datasets/(?P<dataset_id>[a-zA-Z0-9_\\-]+)/partitions']

    @schema.validate(
        output_schema={
            "type": "array",
        },
    )
    @coroutine
    def get(self, dataset_id, **kwargs):
        # pylint: disable=unused-argument
        """
        :param dataset_id:dataset identifier
        :return: partitons pertaining to dataset
        """
        try:
            result = yield self.__get_datasets__()
            if result is None:
                raise APIError(503, log_message="Server internal error")
            dataset_found = [i for i in result if i['id'] == dataset_id]
            for dataset in dataset_found:
                logging.info(u'Partition request for dataset:{%s} received', dataset_id)
                result = yield self.__get_parts__(dataset["path"])
                if result is None:
                    raise APIError(503, log_message="Not able to retrieve partitons")
                raise Return(result)
            raise APIError(404, log_message="Dataset by that name not found")
        except Return as return_value:
            raise return_value
        except Exception as exception:
            logging.warn("Exeception thrown in /parts API-> exception msg{%s}", exception.message)
            raise APIError(500, log_message="Server Internal error")


class UpdateDatasets(DataHandler):
    """
    Update/Retrieve  fields pertaining to specific api
    """
    __urls__ = [r'/api/' + API_VERSION + '/datasets/(?P<dataset_id>[a-zA-Z0-9_\\-]+)/?$']

    def __persist_dataset(self, dataset, retention):
        entry = copy.deepcopy(dataset)
        logging.info('Update dataset for following values %s', entry)
        logging.debug("Retention set %s", retention)
        if retention:
            entry[DATASET.RETENTION] = retention
        self.__write_data__(entry)

    @staticmethod
    def __update_policy(dataset, request_data):
        policy = request_data[DATASET.POLICY]
        if policy == POLICY.AGE and DATASET.MAX_AGE in request_data:
            dataset[DATASET.POLICY] = policy
            remove_keys_from_dict(dataset, [DATASET.MAX_SIZE])
            retention = str(request_data[DATASET.MAX_AGE])
            dataset[DATASET.MAX_AGE] = request_data[DATASET.MAX_AGE]
        elif policy == POLICY.SIZE and DATASET.MAX_SIZE in request_data:
            dataset[DATASET.POLICY] = policy
            retention = str(request_data[DATASET.MAX_SIZE])
            remove_keys_from_dict(dataset, [DATASET.MAX_AGE])
            dataset[DATASET.MAX_SIZE] = request_data[DATASET.MAX_SIZE]
        else:
            raise APIError(400, log_message="Not a valid request")
        return retention

    def __update_dataset(self, dataset, request_data):
        logging.info(u'Update request for api:{%s} received', dataset)
        retention = ""
        # Handle policy change
        # When time permits, refactor this one.Violates DRY
        if DATASET.POLICY in request_data:
            retention = self.__update_policy(dataset, request_data)
        # Handle mode change
        if DATASET.MODE in request_data:
            if request_data[DATASET.MODE] in MODE_ENUM_LIST:
                dataset[DATASET.MODE] = request_data[DATASET.MODE]
            else:
                raise APIError(400, log_message="Not a valid request with invalid mode")
        self.__persist_dataset(dataset, retention)
        raise Return(dataset)

    @schema.validate(
        output_schema=DATASET_SCHEMA
    )
    @coroutine
    def get(self, dataset_id, **kwargs):
        # pylint: disable=unused-argument
        """
        :param dataset_id:
        :return:
        """
        try:
            result = yield self.__get_datasets__()
            if result is None:
                raise APIError(500, log_message="Server internal error")
            dataset_found = [i for i in result if i['id'] == dataset_id]
            if len(dataset_found) == 0:
                raise APIError(404, log_message="Dataset by that name not found.")
            logging.info("dataset found %s", dataset_found)
            raise Return(dataset_found.pop())
        except Return as return_value:
            raise return_value
        except Exception as exception:
            logging.warn("Exception thrown in /id API %s", exception.message)
            raise APIError(500, log_message="Server Internal error")

    @schema.validate(
        output_schema=DATASET_SCHEMA
    )
    @coroutine
    def put(self, dataset_id, **kwargs):
        # pylint: disable=unused-argument
        """
        Update or Create dataset. In case of creation, dataset is validated against
        input schema
        :param dataset_id:Dataset identifier
        :return:
        """
        try:
            result = yield self.__get_datasets__()
            if result is None:
                raise APIError(500, log_message="Server internal error")

            dataset_found = [item for item in result if item[DATASET.ID] == dataset_id]
            if dataset_found:
                request_data = escape.json_decode(self.request.body)
                self.__update_dataset(dataset_found[0], request_data)
            else:
                item = escape.json_decode(self.request.body)
                item["id"] = dataset_id
                try:
                    jsonschema.validate(item, DATASET_SCHEMA)
                    retention = self.__update_policy(item, item)
                    self.__persist_dataset(item, retention)
                    raise Return(item)
                except jsonschema.ValidationError as ex:
                    logging.error("Failed to validate input schema {msg:%s}", ex.message)
                    raise APIError(400, log_message="Malformed request")
                except jsonschema.SchemaError as ex:
                    logging.error("Failed to validate input schema {msg:%s}", ex.message)
                    raise APIError(400, log_message="Malformed request")
        except Return as return_exception:
            raise return_exception
        except APIError as api_error:
            raise api_error
        except Exception as exception:
            logging.warn('Dataset updated returned with following error '
                         'on server->%s', exception.message)
            raise APIError(500, log_message="Internal Server Error")
