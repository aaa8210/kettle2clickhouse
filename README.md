# kettle2clickhouse
kettle connect clickhouse  plug

kettle JDBC clickhouse 

1.支持源码class+依赖jar打包成KettleClickhouseJDBC-{pdi}如KettleClickhouseJDBC-9.1.0.0-324，上传到kettle的plugin/clickhouse-plugin目录下。
 解压出来的jar目录如下:
 
`
 KettleClickhouseJDBC
   org
   *.jar
不需要解压
'

2.升级clickhouse-jdbc到0.3.0，上传到 libswt/win64/

3.lz4-1.3.0上传到 libswt/win64/，否则会报Could not initialize class ru.yandex.clickhouse.response.ClickHouseLZ4Stream




参考原来的说明
kettle : 8.1

clickhouse : 19.15.1.4

CSDN: https://blog.csdn.net/aaa8210/article/details/110632472

ps: kettle8 可操作界面没有clickhouse 数据源，该插件为 clickhouse 数据源插件


