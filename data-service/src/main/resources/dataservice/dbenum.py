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
   Purpose: Enum constants for Table schema and Dataset keys .
"""


class EnumDict(object):
    """An enumeration class."""
    _dict = None

    @classmethod
    def dict(cls):
        """Dictionary of all upper-case constants."""
        if cls._dict is None:
            val = lambda x: getattr(cls, x)
            cls._dict = dict(((c, val(c)) for c in dir(cls)
                              if c == c.upper()))
        return cls._dict

    def __contains__(self, value):
        return value in self.dict().values()

    def __iter__(self):
        for value in self.dict().values():
            yield value


class DATASET(EnumDict):
    """ Dataset schema constants """
    ID = 'id'
    MODE = 'mode'
    PATH = 'path'
    POLICY = 'policy'
    MAX_AGE = 'max_age_days'
    MAX_SIZE = 'max_size_gigabytes'
    RETENTION = 'retention'
    INTEGRITY_ERROR = 'integrity_error'

class DBSCHEMA(EnumDict):
    """ HBase schema constants """
    ID = 'id'
    PATH = 'cf:path'
    POLICY = 'cf:policy'
    RETENTION = 'cf:retention'
    MODE = 'cf:mode'


class POLICY(EnumDict):
    """ Policy constants"""
    AGE = 'age'
    SIZE = 'size'
