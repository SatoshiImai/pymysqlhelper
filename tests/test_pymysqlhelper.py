# coding:utf-8
# ---------------------------------------------------------------------------
# __author__ = 'Satoshi Imai'
# __credits__ = ['Satoshi Imai']
# __version__ = '0.9.0'
# ---------------------------------------------------------------------------

import json
import logging
from logging import Logger, StreamHandler
from pathlib import Path
from typing import Any, Generator
from unittest.mock import Mock, patch

import boto3
import numpy as np
import pandas as pd
import pytest

from src.pymysqlhelper import pymysqlhelper


@pytest.fixture(scope='session', autouse=True)
def setup_and_teardown():
    # setup

    yield

    # teardown
    # end def


@pytest.fixture(scope='module')
def logger() -> Generator[Logger, None, None]:
    log = logging.getLogger(__name__)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s : %(message)s')
    s_handler = StreamHandler()
    s_handler.setLevel(logging.INFO)
    s_handler.setFormatter(formatter)
    log.addHandler(s_handler)

    yield log
    # end def


@pytest.mark.run(order=10)
@pytest.mark.parametrize('value_list,expected', [
    ([1.1, 2, 3, 4], (1.1, 2, 3, 4)),
    ([np.float32(np.nan), np.float64(np.nan),
     np.int32(3), np.int64(4)], (None, None, 3, 4)),
    ([np.float32(1.1), np.float64(2),
     np.int32(3), np.int64(4)], (float(np.float32(1.1)), 2, 3, 4)),
    ([np.float32(2), np.float64(1.1),
     np.int32(3), np.int64(4)], (2, 1.1, 3, 4)),
    (['1', '2', '3', '4'], ('1', '2', '3', '4')),
    (list(pd.array([1, 2, np.nan, np.nan], dtype='Int64')),
     (1, 2, None, None)),
    ([pd.Timestamp(2017, 1, 1), '2', '3', '4'], (pd.Timestamp(2017, 1, 1).to_pydatetime(), '2', '3', '4'))])
def test_to_sqlparams(value_list: Any, expected: Any, logger: Logger):

    logger.info(f'Test convert: {value_list}')

    assert expected == pymysqlhelper.to_sqlparams(value_list)
    # end def


@pytest.mark.run(order=20)
def test_get_secret_connection(logger: Logger):
    test_query = 'SELECT * FROM sys.version;'

    my_client = Mock()
    my_client.get_secret_value.return_value = {'Name': 'dummy',
                                               'SecretString': json.dumps({'username': 'root',
                                                                           'password': 'root',
                                                                           'engine': 'mysql',
                                                                           'host': 'dummy',
                                                                           'port': 3306,
                                                                           'dbClusterIdentifier': 'dummy',
                                                                           'dbname': 'sys',
                                                                           'host_name': 'dummy'})
                                               }

    my_session = Mock()
    my_session.client.return_value = my_client

    with patch.object(boto3, 'Session', return_value=my_session):
        conn = pymysqlhelper.get_secret_connection(
            'dummy',
            host_override='localhost',
            port_override=3320,
            ca_file=Path('dummy'),
            connect_timeout=30)

        cursor = conn.cursor()
        try:
            return_value = cursor.execute(test_query)
            logger.info(return_value)
            assert return_value == 1
        finally:
            if cursor is not None:
                cursor.close()
                # end if
            if conn is not None:
                conn.close()
                # end if
            # end try
        # end with
    # end def


@pytest.mark.run(order=30)
def test_get_secret_sqlalchemy_engine(logger: Logger):
    test_query = 'SELECT * FROM sys.version;'

    my_client = Mock()
    my_client.get_secret_value.return_value = {'Name': 'dummy',
                                               'SecretString': json.dumps({'username': 'root',
                                                                           'password': 'root',
                                                                           'engine': 'mysql',
                                                                           'host': 'dummy',
                                                                           'port': 3306,
                                                                           'dbClusterIdentifier': 'dummy',
                                                                           'dbname': 'sys',
                                                                           'host_name': 'dummy'})
                                               }

    my_session = Mock()
    my_session.client.return_value = my_client

    with patch.object(boto3, 'Session', return_value=my_session):
        engine = pymysqlhelper.get_secret_sqlalchemy_engine(
            'dummy',
            host_override='localhost',
            port_override=3320,
            connect_timeout=30)

        this_df = pd.read_sql(test_query, engine)
        logger.info(this_df)
        assert len(this_df) == 1
        # end with
    # end def


@pytest.mark.run(order=40)
def test_get_secret_sqlalchemy_engine_ca(logger: Logger):
    test_query = 'SELECT * FROM sys.version;'

    my_client = Mock()
    my_client.get_secret_value.return_value = {'Name': 'dummy',
                                               'SecretString': json.dumps({'username': 'root',
                                                                           'password': 'root',
                                                                           'engine': 'mysql',
                                                                           'host': 'dummy',
                                                                           'port': 3306,
                                                                           'dbClusterIdentifier': 'dummy',
                                                                           'dbname': 'sys',
                                                                           'host_name': 'dummy'})
                                               }

    my_session = Mock()
    my_session.client.return_value = my_client

    with patch.object(boto3, 'Session', return_value=my_session):
        with pytest.raises(FileNotFoundError):
            engine = pymysqlhelper.get_secret_sqlalchemy_engine(
                'dummy',
                host_override='localhost',
                port_override=3320,
                ca_file=Path('dummy'),
                connect_timeout=30)

            pd.read_sql(test_query, engine)
            # end with
        # end with
    # end def
