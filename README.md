# apache-hive-2.3.7-src

```
CREATE TABLE `hive_lineage_log` (`id` int(11) NOT NULL , `lineage_str` text DEFAULT NULL COMMENT '【维度】一级来源' KEY `idx_id` (`id`)) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='hive血缘关系日志';


INSERT INTO hive_lineage_log(lineage_str) VALUES ('test');

cp -r ~/workspace4os/apache-hive-2.3.7-src/packaging/target/apache-hive-2.3.7-bin/apache-hive-2.3.7-bin .

cd apache-hive-2.3.7-bin

cp /Users/qjiang/_AllDocMap/00_Private/JamesTraining/Hive/conf/* conf/.

cp /Users/qjiang/_AllDocMap/00_Private/JamesTraining/Hive/lib/* lib/.

hive --service metastore 2>&1 >> ./std.out &
```
