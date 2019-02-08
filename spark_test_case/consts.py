SPARK_DEFAULT_CONF = {
    'spark.master': 'local',
    'spark.default.parallelism': '1',
    'spark.sql.shuffle.partitions': '1'
}

IGNORE_DIRS = ['\\.git', '\\.idea']
EXTENSIONS_TO_WATCH = ['py', 'json']
