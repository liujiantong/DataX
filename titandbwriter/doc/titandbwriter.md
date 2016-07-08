## Datax TitanDBWriter

### 1 快速介绍
TitanDBWriter插件利用TitanDB 的java客户端进行TitanDB的写操作。
### 2 实现原理

TitanDBWriter通过Datax Reader 获得数据，然后将关系数据库表记录的字段逐一转换成用TitanDB Vertex和Edge 表示的关联数据模型。之后依托于Datax框架并行的写入TitanDB。

### 3 功能说明

#### 配置样例
* 该示例从OracleReader读一份数据到TitanDB。
```json
{
    "job": {
        "setting": {
            "speed": {
                "channel": 3
            }
        },
        "content": [
            {
                "reader": {
                    "name": "oraclereader",
                    "parameter": {
                        "username": "root",
                        "password": "root",
                        "column": [
                            "id","name"
                        ],
                        "splitPk": "db_id",
                        "connection": [
                            {
                                "table": [
                                    "table"
                                ],
                                "jdbcUrl": [
                                    "jdbc:oracle:thin:@[HOST_NAME]:PORT:[DATABASE_NAME]"
                                ]
                            }
                        ]
                    }
                },
                "writer": {
                    "name": "titandbwriter",
                    "parameter": {
                        "writeMode": "insert",
                        "username": "root",
                        "password": "root",
                        "column": [
                            "id",
                            "name"
                        ],
                        "session": [
                        	"set session sql_mode='ANSI'"
                        ],
                        "preSql": [
                            "delete from test"
                        ],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://127.0.0.1:3306/datax?useUnicode=true&characterEncoding=gbk",
                                "table": [
                                    "test"
                                ]
                            }
                        ]
                    }
                }
            }
        ]
    }
}

```
