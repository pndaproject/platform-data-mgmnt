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
   Purpose: Run jobs periodically to clean log files and manage datasets as per policy
"""

import json
import logging
from logging.config import fileConfig
import os
import posixpath as path
import re
import subprocess
import time
import traceback
from functools import partial
from functools import wraps

import happybase
import swiftclient
from pyhdfs import HdfsClient, HdfsFileNotFoundException
import boto.s3

from endpoint import Platform

NEG_SIZE = 2
FNULL = open(os.devnull, 'w')


def delete(hdfs, file_path):
    """
    Delete file from HDFS Filesystem
    :param hdfs:
    :param file_path:
    :return:
    """
    logging.debug("Delete HDFS File:%s", file_path)
    hdfs.delete(file_path)


def archive(container_path, hdfs, file_path):
    """
    Archive contents of file onto swift container
    :param container_path:
    :param hdfs:
    :param file_path:
    :return:
    """
    logging.info("Archive file onto swift container %s", file_path)
    archive_path = container_path
    try:
        file_date = re.findall(r"=(\w*)", file_path)
        if file_date:
            subprocess.call(['hdfs', 'dfs', '-mkdir', container_path + '/' + file_date[0]], stderr=FNULL)
            archive_path = path.join(container_path, file_date[0], '-'.join(file_date) + '-' + path.basename(file_path))
        logging.info("swift archive path %s", archive_path)
        subprocess.check_output(['hdfs', 'dfs', '-cp', file_path, archive_path])
        delete(hdfs, file_path)
    except subprocess.CalledProcessError as cpe:
        logging.error('CPE:failed to archive {%s} with following error{%s}', file_path, cpe.message)
    except ValueError as value_error:
        logging.error('VE:failed to archive {%s} with following error{%s}', file_path,
                      value_error.message)


def check_threshold():
    """
    Check threshold value
    :return: True
    """

    def decorator(func):
        """
        Decorator
        :param func:
        :return:
        """

        @wraps(func)
        def wrapper(*args, **kwargs):
            """
            Wrapper function
            :param args:
            :param kwargs:
            :return:
            """
            result = func(*args, **kwargs)
            logging.debug("file modification time={%s} and age={%s}", result, args[0] * 1000)
            if result <= (args[0] * 1000):
                return True
        return wrapper
    return decorator


@check_threshold()
def extract_age(retention_age, hdfs, name):
    # pylint: disable=unused-argument
    """
    Extract age of a HDFS file, retention age is passed on as argument to decorator function
    and used by check_threshold
    :param retention_age: Age specified since 1970 determines whether file should be
     retained or not
    :param hdfs: Object reference to access HDFS file system
    :param name: name of file
    :return: Last modified
    """
    last_modified = hdfs.get_file_status(name).modificationTime
    return last_modified


def extract_size(hdfs, name):
    """
    Extract size of a HDFS file.
    :param hdfs:
    :param name:
    :return:
    """
    file_size = hdfs.get_file_status(name)['length']
    return file_size


def error(exception):
    """
    Callback function used HDFS module
    :param exception: Exception object
    :return:
    """
    logging.warn("Error in HDFS API Invocation error msg->{%s}", exception.message)


def clean_empty_dirs(hdfs, root, dirs):
    for dir_entry in dirs:
        abspath = path.join(root, dir_entry)
        if hdfs.get_content_summary(abspath).fileCount < 1:
            # The directory will not be removed if not empty
            logging.debug("Delete directory:->{%s} as its empty", dir_entry)
            hdfs.delete(abspath)


def cleanup_on_age(hdfs, cmd, clean_path, age):
    """
    Clean up files when it ages as determined by threshold
    :param hdfs: hdfs instance
    :param cmd: cmd to run when threshold is reached
    :param clean_path: repo path
    :param age: Threshold value in this case age
    :return: None
    """
    dir_list = clean_path
    if not isinstance(clean_path, list):
        dir_list = list()
        dir_list.append(clean_path)

    for dir_to_clean in dir_list:
        for root, dirs, files in hdfs.walk(dir_to_clean, topdown=False, onerror=error):
            logging.info("Root:{%s}->Dirs:{%s}->Files:{%s}", root, dirs, files)
            for filename in files:
                abspath = path.join(root, filename)
                if extract_age(age, hdfs, abspath):
                    cmd(abspath)
            clean_empty_dirs(hdfs, root, dirs)

def cleanup_on_size(hdfs, cmd, clean_path, size_threshold):
    """
    Clean up hdfs data directories when threshold is reached

    :param hdfs: hdfs instance for file walk
    :param cmd: cmd to run when threshold is reached. It is usually archive or delete command
    :param clean_path: Path to clean
    :param size_threshold: Threshold value for file repo
    :return: None
    """
    logging.info("Clean following dirs on basis of size [{%s}]", clean_path)
    dir_list = clean_path
    if not isinstance(clean_path, list):
        dir_list = list()
        dir_list.append(clean_path)

    for clean_dir in dir_list:
        try:
            space_consumed = hdfs.get_content_summary(clean_dir).length
            logging.info("Space consumed by directory{%s} on filesystem:{%d} policy threshold:{%d}",
                         clean_dir, space_consumed, size_threshold)
            if space_consumed > size_threshold:
                for root, dirs, files in hdfs.walk(clean_dir, topdown=False,
                                                   onerror=error):
                    logging.info("Root:{%s}->Dirs:{%s}->Files:{%s}", root, dirs, files)
                    for item in files:
                        
                        if space_consumed <= size_threshold:
                            break

                        # Read the file-size from HDFS, remove file and update the space_consumed
                        abspath = path.join(root, item)
                        file_size = extract_size(hdfs, abspath)
                        cmd(abspath)
                        space_consumed -= file_size

                    clean_empty_dirs(hdfs, root, dirs)
        except HdfsFileNotFoundException as hdfs_file_not_found_exception:
            logging.warn("{%s}", hdfs_file_not_found_exception.message)
        except Exception as exception:
            logging.warn("Exception in clean directories possibly dir doesnt exist{%s}",
                         exception.message)

def cleanup_spark(spark_path):
    """
    Clean up spark log and app files
    :param spark_path: filesystem path that contains spark related files
    :return:
    """
    logging.info('Cleaning spark streaming cruft')
    reg = re.compile('/(application_[0-9]*_[0-9]*)(.inprogress)*$')
    for dir_to_consider in spark_path:
        logging.info('cleaning up %s', dir_to_consider)
        try:
            sub_dirs = subprocess.check_output(['hadoop', 'fs', '-ls', dir_to_consider],
                                               stderr=FNULL)
        except subprocess.CalledProcessError:
            logging.warn('failed to ls %s', dir_to_consider)
            continue

        for dir_path_line in sub_dirs.splitlines():
            search_match = reg.search(dir_path_line)
            if search_match:
                app_id = search_match.group(1)
                try:
                    app_status = subprocess.check_output(['yarn', 'application', '-status', app_id],
                                                         stderr=FNULL)
                except subprocess.CalledProcessError:
                    logging.warn(
                        'app probably not known to resource manager for some reason (like yarn was '
                        'restarted)')
                    app_status = 'State : FINISHED'
                dir_path_line_parts = dir_path_line.split(' ')
                dir_path_line_parts = filter(None, dir_path_line_parts)
                dir_path = "%s" % ''.join(dir_path_line_parts[7:])
                if 'State : FINISHED' in app_status or 'State : FAILED' in app_status or \
                                'State : KILLED' in app_status:
                    logging.warn('delete: %s', dir_path)
                    try:
                        subprocess.check_output(
                            ['hadoop', 'fs', '-rm', '-r', '-f', '-skipTrash', dir_path])
                    except subprocess.CalledProcessError:
                        logging.warn('failed to delete: %s', dir_path)
                else:
                    logging.warn('keep: %s', dir_path)


def read_datasets_from_hbase(table_name, hbase_host):
    """
    Connect to hbase table and return list of datasets
    :param table_name:
    :param hbase_host:
    :return:
    """
    logging.info("Connecting to  database to retrieve datasets ")
    datasets = list()

    try:
        connection = happybase.Connection(hbase_host)
        connection.open()
        table = connection.table(table_name, )
        logging.info('connecting to hbase to read data sets')
        for key, data in table.scan():
            logging.debug("Looking for next data in HBase")
            dataset = dict(name=key, path=data['cf:path'], policy=data['cf:policy'],
                           retention=data['cf:retention'], mode=data['cf:mode'])
            if dataset['policy'] == "size":
                dataset['retention'] = int(dataset['retention']) * 1024 * 1024 * 1024
                datasets.append(dataset)
            elif dataset['policy'] == "age":
                # from days to seconds
                age_in_secs = int(dataset['retention']) * 86400
                dataset['retention'] = int(time.time() - age_in_secs)
                datasets.append(dataset)
            else:
                logging.error("Invalid dataset entry in HBase")

    except Exception as exception:
        logging.warn("Exception thrown for datasets walk on HBASE->'{%s}'", exception.message)
    return datasets


class JOB(object):
    """
    The Clean up job instance. It takes in strategy and run as part of schedule or
    cron
    """

    def __init__(self, name, hdfs, strategy, cmd, repo_path, threshold):
        self.name = name
        self.hdfs = hdfs
        self.strategy = strategy
        self.cmd = cmd
        self.path = repo_path
        self.threshold = threshold

    def run(self):
        """
        Run specific job
        :return:
        """
        if hasattr(self.strategy, '__call__'):
            self.strategy(self.hdfs, self.cmd, self.path, self.threshold)


def main():
    """
    Main function of job cleanup module
    :return: none
    """
    # instantiate platform for Cloudera
    # need to be removed, once spark refactoring happens



    jobs = list()
    with file('properties.json') as property_file:
        properties = json.load(property_file)
    platform = Platform.factory(properties['hadoop_distro'])
    # discover endpoints
    endpoints = platform.discover(properties)
    assert endpoints
    fileConfig('logconf.ini')
    logging.info("Discovered following endpoints from cluster manager{%s}", endpoints)

    # setup endpoints
    hdfs = HdfsClient(endpoints["HDFS"].geturl(), user_name='hdfs')
    hbase = endpoints["HBASE"].geturl()

    # Create s3 or swift bucket for archive purposes
    try:
        if properties['s3_archive_region'] != '':
            container_type = 's3'
            s3conn = boto.s3.connect_to_region(properties['s3_archive_region'],
                                               aws_access_key_id=properties['s3_archive_access_key'],
                                               aws_secret_access_key=properties['s3_archive_secret_access_key'])
            s3conn.create_bucket(properties['container_name'], location=properties['s3_archive_region'])
        else:
            container_type = 'swift'
            swift_conn = swiftclient.client.Connection(auth_version='2',
                                                       user=properties['swift_user'],
                                                       key=properties['swift_key'],
                                                       tenant_name=properties['swift_account'],
                                                       authurl=properties['swift_auth_url'],
                                                       timeout=30)
            swift_conn.put_container(properties['container_name'])
            swift_conn.close()
    except Exception as ex:
        # the create container operations are idempotent so would only expect genuine errors here
        logging.error("Failed to create %s container %s", container_type, properties['container_name'])
        logging.error(traceback.format_exc(ex))

    # create partial functions
    delete_cmd = partial(delete, hdfs)
    archive_cmd = partial(archive, properties['swift_repo'], hdfs)

    # clean spark directors
    spark_streaming_dirs_to_clean = properties['spark_streaming_dirs_to_clean']
    cleanup_spark(spark_streaming_dirs_to_clean)

    # general directories to clean
    general_dirs_to_clean = properties['general_dirs_to_clean']
    job_common_dirs = JOB('clean_general_dir', hdfs, cleanup_on_size, delete_cmd,
                          general_dirs_to_clean, NEG_SIZE)
    jobs.append(job_common_dirs)

    old_dirs_to_clean = properties['old_dirs_to_clean']
    for entry in old_dirs_to_clean:
        print entry['name']
        age = int(time.time() - entry['age_seconds'])
        job_old_dirs = JOB('clean_old_dir', hdfs, cleanup_on_age, delete_cmd, entry['name'], age)
        jobs.append(job_old_dirs)

    # # Read all datasets
    data_sets = read_datasets_from_hbase(properties['datasets_table'], hbase)
    for item in data_sets:
        logging.debug("dataset item being scheduled {%s}", item)
        cmd = delete_cmd if 'mode' in item and item["mode"] == "delete" else archive_cmd
        strategy = cleanup_on_age if item['policy'] == "age" else cleanup_on_size
        job = JOB(item['name'], hdfs, strategy, cmd, item['path'], item['retention'])
        jobs.append(job)

    for job in jobs:
        logging.info(job.name)
        job.run()


if __name__ == '__main__':
    main()
