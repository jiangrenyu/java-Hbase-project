# java-hbase-project
流量轨迹数据入库程序以及查询程序
项目需求：
    使用Hbase的java api将流量轨迹数据导入Hbase；入库时要求原始数据要插入哪些字段，向哪些列插入字段值以及Hbase的列族，列名，行键都要可人为配置 
    对Hbase表进行查询时，要求查询的列族和列名可配置，程序支持单个行键查询，范围查询以及全表扫描，同时，HBase导出的数据根据人为配置来决定导入到HDFS
  还是本地磁盘 
