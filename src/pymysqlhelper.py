# coding:utf-8
# ---------------------------------------------------------------------------
# __author__ = 'Satoshi Imai'
# __credits__ = ['Satoshi Imai']
# __version__ = "0.9.0"
# ---------------------------------------------------------------------------

import json
from pathlib import Path
from typing import Any, List, Optional, Tuple

import boto3
import numpy as np
import pandas as pd
import pymysql
from botocore.config import Config
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.engine.base import Engine


class pymysqlhelper(object):

    @classmethod
    def to_sqlparams(cls, params: List[Any]) -> Tuple:
        for index in range(len(params)):
            params[index] = cls.to_sqltype(params[index])
        return tuple(params)
        # end def

    @classmethod
    def to_sqltype(cls, value: Any) -> Any:
        if pd.isnull(value) or pd.isna(value):
            return None
            # end if
        if isinstance(value, float):
            # Do nothing
            pass
        elif isinstance(value, np.float32) or isinstance(value, np.float64):
            value = float(value)
        elif isinstance(value, int):
            # Do nothing
            pass
        elif isinstance(value, np.int32) or isinstance(value, np.int64):
            value = int(value)
        elif isinstance(value, pd.Timestamp):
            value = value.to_pydatetime()
            # end if

        return value
        # end def

    @classmethod
    def get_secret_connection(cls,
                              secret_name: str,
                              profile: str = None,
                              region: str = 'ap-northeast-1',
                              config: Config = None,
                              charset: str = 'utf8',
                              host_override: str = None,
                              port_override: int = -1,
                              ca_file: str = None,
                              connect_timeout: int = -1,
                              **kwargs: Any) -> pymysql.connections.Connection:

        my_session = boto3.Session(region_name=region, profile_name=profile)

        my_client = my_session.client(
            'secretsmanager',
            region_name=region,
            config=config)
        response = my_client.get_secret_value(
            SecretId=secret_name)
        setting = json.loads(response['SecretString'])

        endpoint = setting['host']
        if host_override is not None:
            endpoint = host_override
            # end if
        port = int(setting['port'])
        if port_override > 0:
            port = port_override
            # end if
        db = setting['dbname']
        user = setting['username']
        password = setting['password']

        explicit_args = {}
        if ca_file is not None:
            if isinstance(ca_file, Path):
                ca_file = str(ca_file)
                # end if
            explicit_args['ssl'] = {'ssl': {
                'ca': ca_file
            }}
            # end if
        if connect_timeout > 0:
            explicit_args['connect_timeout'] = connect_timeout
            # end if
        if len(explicit_args) > 0:
            kwargs.update(explicit_args)
            # end if

        connection = pymysql.connect(host=endpoint,
                                     port=port,
                                     db=db,
                                     user=user,
                                     password=password,
                                     charset=charset,
                                     **kwargs)

        return connection
        # end def

    @classmethod
    def get_secret_sqlalchemy_engine(cls,
                                     secret_name: str,
                                     profile: str = None,
                                     region: str = 'ap-northeast-1',
                                     config: Config = None,
                                     charset: str = 'utf8',
                                     host_override: str = None,
                                     port_override: int = -1,
                                     ca_file: str = None,
                                     connect_timeout: int = -1,
                                     **kwargs: Any) -> Optional[Engine]:

        my_session = boto3.Session(region_name=region, profile_name=profile)

        my_client = my_session.client(
            'secretsmanager',
            region_name=region,
            config=config)
        response = my_client.get_secret_value(
            SecretId=secret_name)
        setting = json.loads(response['SecretString'])

        endpoint = setting['host']
        if host_override is not None:
            endpoint = host_override
            # end if
        port = int(setting['port'])
        if port_override > 0:
            port = port_override
            # end if
        db = setting['dbname']
        user = setting['username']
        password = setting['password']

        explicit_args = {}
        explicit_args['charset'] = charset
        if ca_file is not None:
            if isinstance(ca_file, Path):
                ca_file = str(ca_file)
                # end if
            explicit_args['ssl_ca'] = ca_file
            # end if
        if connect_timeout > 0:
            explicit_args['connect_timeout'] = connect_timeout
            # end if
        if len(explicit_args) > 0:
            kwargs.update(explicit_args)
            # end if

        connection_url = URL.create(drivername='mysql+pymysql',
                                    username=user,
                                    password=password,
                                    host=endpoint,
                                    port=port,
                                    database=db)

        engine = create_engine(connection_url, connect_args=kwargs)

        return engine
        # end def

    # end class
