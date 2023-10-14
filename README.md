# pymysqlhelper

Simple code snippets to communicate with MYSQL for a Python project.

## to_sqlparams

Convert data types to pymysql acceptable types to allow passing it to `executemany` like query.

```python
import pandas as pd

from pymysqlhelper import pymysqlhelper

this_df = pd.DataFrame([[1, 2, 3], [1.1, 1.2, 1.3]], columns=['A', 'B', 'C'])

my_params = [
    pymysqlhelper.to_sqlparams(current) for current in
        this_df.values.tolist()    
]
```

## get_secret_connection

A code snippet to generate `pymysql.connections.Connection` object with AWS secrets manager.

```python
from pymysqlhelper import pymysqlhelper

conn = pymysqlhelper.get_secret_connection(
    'your secret name',
    connect_timeout=30)

cursor = conn.cursor()
cursor.execute('your query')

cursor.close()
conn.close()

```

## get_secret_sqlalchemy_engine

A code snippet to generate `sqlalchemy.engine` object with AWS secrets manager.

```python
import pandas as pd

from pymysqlhelper import pymysqlhelper

engine = pymysqlhelper.get_secret_sqlalchemy_engine(
    'your secret name',
    connect_timeout=30)

this_df = pd.read_sql('your query', engine)
```

## LICENSE

I inherited BSD 3-Clause License from [pandas](https://pypi.org/project/pandas/)
