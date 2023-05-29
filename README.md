# 使用步骤
###### 使用环境

Mysql : Mysql 8

Python:3.6以上版本


###### 目的
数据库实例无法启动的情况下，解析ibd文件，进行数据恢复  





#### 1.解析ibd文件
/opt/mysql8/bin/ibd2sdi --dump-file=/tmp/model.txt /tmp/model.ibd


#### 2.获取表结构
python parse_ibd2sdi.py /tmp/model.txt
```c
create table model(
    id int  NOT NULL  AUTO_INCREMENT COMMENT 'id主键',
    model_id int DEFAULT NULL COMMENT '模块的父模块id',
    model_parent_ids varchar(300) DEFAULT NULL COMMENT '父级编号集合，从小到大排序',
    model_code varchar(255) DEFAULT NULL COMMENT '模块编码',
    model_title varchar(150) DEFAULT NULL COMMENT '模块标题',
    model_url varchar(255) DEFAULT NULL COMMENT '模块连接地址',
    model_icon varchar(120) DEFAULT NULL COMMENT '模块图标',
    model_sort int DEFAULT NULL COMMENT '模块的排序',
    model_ismenu int DEFAULT '0' COMMENT '模块是否是菜单',
    model_datetime datetime DEFAULT NULL,
    UPDATE_BY varchar(11) DEFAULT NULL COMMENT '更新人',
    UPDATE_DATE datetime DEFAULT NULL COMMENT '更新时间',
    CREATE_BY varchar(11) DEFAULT NULL COMMENT '创建人',
    CREATE_DATE datetime DEFAULT NULL COMMENT '创建时间',
    DEL int DEFAULT '0' COMMENT '删除标记',
    PRIMARY KEY (id),
    KEY idx_model_id (model_id),
    KEY idx_model_title (model_title,model_url),
    CONSTRAINT fk_model_id FOREIGN KEY(model_id) REFERENCES model(id) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT = COMPACT COMMENT '模块表'

```

### 3.在新环境创建表  
  
  
  




### 4.脱离表空间
```c
mysql> alter table model discard tablespace;
Query OK, 0 rows affected (0.12 sec)
```
这个时候该表对应的ibd文件会自动删除掉


### 5.关闭数据库
`/opt/mysql8/bin/mysqladmin -h localhost -uroot -P13306 --socket=/opt/mysql8/mysql.sock -p shutdown`

### 6.将不能启动的实例下的表对应的ibd文件拷贝到正常的实例的相应目录下
`cp /tmp/model.ibd /opt/mysql8/data/db_test/`

文件传输到目标机器后,注意修改权限

`chown -R mysql:mysql /opt/mysql8/data/db_test`

### 7.启动实例
`/opt/mysql8/bin/mysqld_safe --defaults-file=/opt/mysql8/conf/my.cnf --user=mysql &`

### 8.导入表空间
`/opt/mysql8/bin/mysql -h localhost -uroot -P13306 --socket=/opt/mysql8/mysql.sock -p`

```c
mysql> alter table model import tablespace;
Query OK, 0 rows affected, 1 warning (5.86 sec)
```
