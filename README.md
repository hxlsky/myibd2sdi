# 使用步骤
For Mysql 8
#### 1.解析ibd文件
/opt/mysql8/bin/ibd2sdi --dump-file=/tmp/cms_content.txt /tmp/model.ibd


#### 2.获取表结构
python parse_ibd2sdi.py /tmp/model.txt
```c
CREATE TABLE `model` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id主键',
  `model_id` int(22) DEFAULT NULL COMMENT '模块的父模块id',
  `model_parent_ids` varchar(300) DEFAULT NULL COMMENT '父级编号集合，从小到大排序',
  `model_code` varchar(255) DEFAULT NULL COMMENT '模块编码',
  `model_title` varchar(150) DEFAULT NULL COMMENT '模块标题',
  `model_url` varchar(255) DEFAULT NULL COMMENT '模块连接地址',
  `model_icon` varchar(120) DEFAULT NULL COMMENT '模块图标',
  `model_sort` int(11) DEFAULT NULL COMMENT '模块的排序',
  `model_ismenu` int(1) DEFAULT '0' COMMENT '模块是否是菜单',
  `model_datetime` datetime DEFAULT NULL,
  `UPDATE_BY` varchar(11) DEFAULT NULL COMMENT '更新人',
  `UPDATE_DATE` datetime DEFAULT NULL COMMENT '更新时间',
  `CREATE_BY` varchar(11) DEFAULT NULL COMMENT '创建人',
  `CREATE_DATE` datetime DEFAULT NULL COMMENT '创建时间',
  `DEL` int(1) DEFAULT '0' COMMENT '删除标记',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `idx_model_id` (`model_id`) USING BTREE,
  KEY `idx_model_title` (`model_title`,`model_url`) USING BTREE,
  CONSTRAINT `fk_model_id` FOREIGN KEY (`model_id`) REFERENCES `model` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=1859 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT COMMENT='模块表';
```

### 3.在新环境创建表


### 4.脱离表空间
```c
mysql> alter table model discard tablespace;
Query OK, 0 rows affected (0.12 sec)
```
