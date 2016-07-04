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
   Purpose: Implements Restful API service for managing platform dataset
"""

import logging
import signal
import socket

import tornado.httpserver
import tornado.ioloop
from tornado.options import options, parse_config_file
from tornado_json.application import Application
from tornado_json.routes import get_routes

import config
import dataservice
from dataservice import HDBDataStore
from endpoint import Platform
from endpoint import CLOUDERA



def sig_handler(sig, frame):
    """
    call back handler for sighup and sigterm
    :param sig:
    :param frame:
    :return:
    """
    logging.warning(
        "Received shutdown signal for dataset dataset with signal:%s and frame:%s", sig, frame)
    tornado.ioloop.IOLoop.instance().add_callback(shutdown)


def shutdown():
    """shuts down the server"""
    logging.info('Stopping http server')
    apiserver.stop()
    io_loop = tornado.ioloop.IOLoop.instance()
    io_loop.stop()



def main():
    """
    Main entry point for my service.
    :return:
    """
    global apiserver
    config.define_options()
    # Attempt to load config from config file
    try:
        parse_config_file("server.conf")
    except IOError:
        errmsg = ("{} doesn't exist or couldn't be opened. Using defaults."
                  .format(options.conf_file_path))
        logging.warn(errmsg)
    logging.info(options.as_dict())
    platform = Platform.factory(CLOUDERA)
    endpoints = platform.discover(options)
    if not endpoints:
        logging.error("Failed to discover API endpoints of cluster")

    db_store = HDBDataStore(endpoints['HDFS'].geturl(), endpoints['HBASE'].geturl(),
                            options.thrift_port,
                            options.datasets_table,
                            options.data_repo)
    routes = get_routes(dataservice)
    logging.info("Service Routes %s", routes)
    settings = dict()
    apiserver = tornado.httpserver.HTTPServer(
        Application(routes=routes, settings=settings, db_conn=db_store))
    for port in options.ports:
        try:
            logging.debug("Attempting to bind for dataset dataset on port:%d and address %s",
                          port, options.bind_address)
            apiserver.listen(port, options.bind_address)
            logging.info("Awesomeness is listening on:%s", port)
            break
        except socket.error:
            logging.warn("Not able to bind on port:%d", port)
    else:
        logging.warn("No free port available to bind dataset")

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    # keep collecting dataset
    tornado.ioloop.PeriodicCallback(db_store.collect, options.sync_period).start()
    # db_conn2.collect()
    tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':
    main()
