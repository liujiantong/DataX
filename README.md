![Datax-logo](https://github.com/alibaba/DataX/blob/master/images/DataX-logo.jpg)


# DataX

- DataX 是阿里巴巴被广泛使用的离线数据同步工具/平台，实现包括 MySQL、Oracle、SqlServer、Postgre、HDFS、Hive、ADS、HBase、OTS、ODPS 等各种异构数据源之间高效的数据同步功能。
- 为实现爱康国宾的DMO项目, 我们在阿里巴巴 DataX的基础上开发了 TitanDB 图数据库插件, 用于支持DMO项目的数据清洗和集成工作.


# Features

DataX本身作为数据同步框架，将不同数据源的同步抽象为从源头数据源读取数据的Reader插件，以及向目标端写入数据的Writer插件，理论上DataX框架可以支持任意数据源类型的数据同步工作。同时DataX插件体系作为一套生态系统, 每接入一套新数据源该新加入的数据源即可实现和现有的数据源互通。


# DataX详细介绍

##### 请参考：[DataX-Introduction](https://github.com/alibaba/DataX/wiki/DataX-Introduction)

# DataX插件开发指南

##### 版本较老仅供参考：[DataX-Plugin-Dev-Introduction](http://code.taobao.org/p/datax/wiki/index/)

# Quick Start

```
$ git clone git@github.com:alibaba/DataX.git
$ cd  {DataX_source_code_home}
$ mvn clean package assembly:assembly -Dmaven.test.skip=true
$ cd ./target/datax/datax/
$ python bin/datax.py job/titanjob.json

```

##### 详情请点击：[Quick Start](https://github.com/alibaba/DataX/wiki/Quick-Start)


# Support Data Channels

请点击：[DataX数据源参考指南](https://github.com/alibaba/DataX/wiki/DataX-all-data-channels)


# Contact me

邮箱：liujiantong@gmail.com
