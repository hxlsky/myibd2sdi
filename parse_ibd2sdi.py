#!/usr/bin/python
# -*- coding=utf-8 -*-

import json
import sys

##外键更新规则
def gente_row_format_rule():
    row_format_rule_dic={}
    row_format_rule_dic[2] = "DYNAMIC"
    row_format_rule_dic[3] = "COMPRESSED"
    row_format_rule_dic[4] = "REDUNDANT"
    row_format_rule_dic[5] = "COMPACT"
    return row_format_rule_dic

##外键更新规则
def gente_update_rule():
    update_rule_dic={}
    update_rule_dic[1] = "ON UPDATE NO ACTION"
    update_rule_dic[2] = "ON UPDATE RESTRICT"
    update_rule_dic[3] = "ON UPDATE CASCADE"
    update_rule_dic[4] = "ON UPDATE SET NULL"
    update_rule_dic[5] = "ON UPDATE SET DEFAULT"
    return update_rule_dic

##外键删除规则
def gente_delete_rule():
    delete_rule_dic={}
    delete_rule_dic[1] = "ON DELETE NO ACTION"
    delete_rule_dic[2] = "ON DELETE RESTRICT"
    delete_rule_dic[3] = "ON DELETE CASCADE"
    delete_rule_dic[4] = "ON DELETE SET NULL"
    delete_rule_dic[5] = "ON DELETE SET DEFAULT"
    return delete_rule_dic

##字符集
def gente_character_rule():
    character_rule_dic = {}
    character_rule_dic[45] = "utf8mb4"
    character_rule_dic[255] = "utf8mb4"
    character_rule_dic[33] = "utf8"
    character_rule_dic[8] = "latin1"

    return character_rule_dic


##分区
def gente_partition_sql(data):
    partition_type = data[1]["object"]["dd_object"]["partition_type"]
    ##print(partition_type)
    partition_expression = data[1]["object"]["dd_object"]["partition_expression_utf8"]
    sub_partition_expression = data[1]["object"]["dd_object"]["subpartition_expression_utf8"]
    partitions_list = data[1]["object"]["dd_object"]["partitions"]
    subpartition_type=data[1]["object"]["dd_object"]["subpartition_type"]
    default_subpartitioning=data[1]["object"]["dd_object"]["default_subpartitioning"]
    ##print(default_subpartitioning)
    partion_cnt = len(partitions_list)
    partion_sql = ""

    ## 没有子分区的情况
    if subpartition_type==0:
        if partition_type == 1: ## HASH
            partion_sql = "PARTITION BY HASH ("+ partition_expression +") PARTITIONS " +  str(partion_cnt)

        if partition_type == 3: ## KEY()
            partion_sql = "PARTITION BY KEY("+partition_expression+") PARTITIONS " + str(partion_cnt)

        if partition_type == 6: ## LINEAR KEY()
            partion_sql = "PARTITION BY LINEAR KEY("+partition_expression+") PARTITIONS " + str(partion_cnt)

        if partition_type == 7: ## RANGE
            partion_sql = ""
            partion_sql_tmp = "PARTITION BY RANGE (" + partition_expression + ")"
            part_values_sql = ""
            part_values_sql_tmp = ""
            for i  in partitions_list:
                p_name = i["name"]
                p_value = i["description_utf8"]
                part_values_sql_tmp = " PARTITION " + p_name + " VALUES LESS THAN(" + p_value + "),\n"
                ##for j in i["values"]:
                ##    part_values_sql_tmp = " PARTITION " +p_name +" VALUES LESS THAN(" +p_value+"),\n"
                part_values_sql = part_values_sql + part_values_sql_tmp
            ##print(part_values_sql)
            partion_sql = partion_sql_tmp + "\n(\n" + part_values_sql.strip(",\n") + "\n)"
        ##print(partion_sql)

        if partition_type == 9: ## RANGE COLUMNS
            partion_sql = ""
            partion_sql_tmp = "PARTITION BY RANGE COLUMNS(" + partition_expression + ")"
            part_values_sql = ""
            part_values_sql_tmp = ""
            for i  in partitions_list:
                p_name = i["name"]
                p_value = i["description_utf8"]
                part_values_sql_tmp = " PARTITION " + p_name + " VALUES LESS THAN (" + p_value + "),\n"
                ##for j in i["values"]:
                ##    part_values_sql_tmp = " PARTITION " +p_name +" VALUES LESS THAN(" +p_value+"),\n"
                part_values_sql = part_values_sql + part_values_sql_tmp
            ##print(part_values_sql)
            partion_sql = partion_sql_tmp + "\n(\n" + part_values_sql.strip(",\n") + "\n)"
        ##print(partion_sql)



        if partition_type == 8: ## LIST
            partion_sql = ""
            partion_sql_tmp = "PARTITION BY LIST (" + partition_expression + ")"
            part_values_sql = ""
            part_values_sql_tmp = ""
            for i  in partitions_list:
                p_name = i["name"]
                p_value = i["description_utf8"]
                part_values_sql_tmp = " PARTITION " + p_name + " VALUES IN (" + p_value + "),\n"
                ##for j in i["values"]:
                ##    part_values_sql_tmp = " PARTITION " +p_name +" VALUES LESS THAN(" +p_value+"),\n"
                part_values_sql = part_values_sql + part_values_sql_tmp
            ##print(part_values_sql)
            partion_sql = partion_sql_tmp + "\n(\n" + part_values_sql.strip(",\n") + "\n)"
        ##print(partion_sql)


        if partition_type == 10: ## LIST COLUMNS
            partion_sql = ""
            partion_sql_tmp = "PARTITION BY LIST COLUMNS(" + partition_expression + ")"
            part_values_sql = ""
            part_values_sql_tmp = ""
            for i  in partitions_list:
                p_name = i["name"]
                p_value = i["description_utf8"]
                part_values_sql_tmp = " PARTITION " + p_name + " VALUES IN (" + p_value + "),\n"
                ##for j in i["values"]:
                ##    part_values_sql_tmp = " PARTITION " +p_name +" VALUES LESS THAN(" +p_value+"),\n"
                part_values_sql = part_values_sql + part_values_sql_tmp
            ##print(part_values_sql)
            partion_sql = partion_sql_tmp + "\n(\n" + part_values_sql.strip(",\n") + "\n)"
        ##print(partion_sql)

    ##有子分区
    if subpartition_type > 0:
        sub_partitions_list = data[1]["object"]["dd_object"]["partitions"][0]["subpartitions"]
        ##print(sub_partitions_list)
        sub_partitions_cnt  = len(sub_partitions_list)

        if (partition_type == 7) and (subpartition_type in [1,3,6] ): ## RANGE + HASH/key
            partion_sql = ""
            if subpartition_type == 1: ##hash
                if default_subpartitioning == 1: ##自定义分区名称
                    partion_sql_tmp = "PARTITION BY RANGE (" + partition_expression + ")\n" + "SUBPARTITION BY HASH( " + sub_partition_expression + ")"
                else:
                    partion_sql_tmp = "PARTITION BY RANGE (" + partition_expression + ")\n" + "SUBPARTITION BY HASH( " + sub_partition_expression + ")\nSUBPARTITIONS " + str(sub_partitions_cnt)

            if subpartition_type == 3: ## key
                if default_subpartitioning == 1: ##自定义分区名称
                    partion_sql_tmp = "PARTITION BY RANGE (" + partition_expression + ")\n" + "SUBPARTITION BY KEY( " + sub_partition_expression + ")"
                else:
                    partion_sql_tmp = "PARTITION BY RANGE (" + partition_expression + ")\n" + "SUBPARTITION BY KEY( " + sub_partition_expression + ")\nSUBPARTITIONS " + str(sub_partitions_cnt)

            if subpartition_type == 6: ## LINEAR key
                if default_subpartitioning == 1: ##自定义分区名称
                    partion_sql_tmp = "PARTITION BY RANGE (" + partition_expression + ")\n" + "SUBPARTITION BY LINEAR KEY( " + sub_partition_expression + ")"
                else:
                    partion_sql_tmp = "PARTITION BY RANGE (" + partition_expression + ")\n" + "SUBPARTITION BY LINEAR KEY( " + sub_partition_expression + ")\nSUBPARTITIONS " + str(sub_partitions_cnt)

            part_values_sql = ""
            part_values_sql_tmp = ""
            for i  in partitions_list:
                p_name = i["name"]
                p_value = i["description_utf8"]

                ##for j in i["values"]:
                ##    part_values_sql_tmp = " PARTITION " +p_name +" VALUES LESS THAN(" +p_value+"),\n"
                ##遍历子分区
                sub_partition_sql_tmp = ""
                sub_partition_sql = ""
                for j in i["subpartitions"]:
                    sub_partition_sql_tmp = "  SUBPARTITION " + j["name"]+",\n"
                    sub_partition_sql = sub_partition_sql+sub_partition_sql_tmp
                sub_partition_sql_last = sub_partition_sql.strip(",\n ")


                if default_subpartitioning == 1: ##自定义子分区名
                    part_values_sql_tmp = " PARTITION " + p_name + " VALUES LESS THAN(" + p_value + ")\n (" +sub_partition_sql_last+"),\n"
                else:
                    part_values_sql_tmp = " PARTITION " + p_name + " VALUES LESS THAN(" + p_value + "),\n"

                part_values_sql = part_values_sql + part_values_sql_tmp
            ##print(part_values_sql)

            partion_sql = partion_sql_tmp + "\n(\n" + part_values_sql.strip(",\n") + "\n)"

        ##print(partion_sql)



        if (partition_type == 8) and (subpartition_type in [1,3,6]): ## LIST + HASH/KEY/LINEAR KEY
            partion_sql = ""
            if subpartition_type == 1: ##HASH
                if default_subpartitioning == 1:  ##自定义分区名称
                    partion_sql_tmp = "PARTITION BY LIST (" + partition_expression + ")\n" + "SUBPARTITION BY HASH( " + sub_partition_expression + ")"
                else:
                    partion_sql_tmp = "PARTITION BY LIST (" + partition_expression + ")\n" + "SUBPARTITION BY HASH( " + sub_partition_expression + ")\nSUBPARTITIONS " + str(sub_partitions_cnt)

            if subpartition_type == 3: ##KEY
                 if default_subpartitioning == 1: ##自定义分区名称
                    partion_sql_tmp = "PARTITION BY LIST (" + partition_expression + ")\n" + "SUBPARTITION BY LINEAR KEY( " + sub_partition_expression + ")"
                 else:
                    partion_sql_tmp = "PARTITION BY LIST (" + partition_expression + ")\n" + "SUBPARTITION BY LINEAR KEY( " + sub_partition_expression + ")\nSUBPARTITIONS " + str(sub_partitions_cnt)

            if subpartition_type == 6: ##LINEAR EKY
                 if default_subpartitioning == 1: ##自定义分区名称
                    partion_sql_tmp = "PARTITION BY LIST (" + partition_expression + ")\n" + "SUBPARTITION BY LINEAR KEY( " + sub_partition_expression + ")"
                 else:
                    partion_sql_tmp = "PARTITION BY LIST (" + partition_expression + ")\n" + "SUBPARTITION BY LINEAR KEY( " + sub_partition_expression + ")\nSUBPARTITIONS " + str(sub_partitions_cnt)

            part_values_sql = ""
            part_values_sql_tmp = ""
            for i  in partitions_list:
                p_name = i["name"]
                p_value = i["description_utf8"]
                ##遍历子分区
                sub_partition_sql_tmp = ""
                sub_partition_sql = ""
                for j in i["subpartitions"]:
                    sub_partition_sql_tmp = "  SUBPARTITION " + j["name"] + ",\n"
                    sub_partition_sql = sub_partition_sql + sub_partition_sql_tmp
                sub_partition_sql_last = sub_partition_sql.strip(",\n ")

                if default_subpartitioning == 1:  ##自定义子分区名
                    part_values_sql_tmp = " PARTITION " + p_name + " VALUES IN(" + p_value + ")\n (" + sub_partition_sql_last + "),\n"
                else:
                    part_values_sql_tmp = " PARTITION " + p_name + " VALUES IN(" + p_value + "),\n"

                part_values_sql = part_values_sql + part_values_sql_tmp
            ##print(part_values_sql)

            partion_sql = partion_sql_tmp + "\n(\n" + part_values_sql.strip(",\n") + "\n)"


    return partion_sql

##外键
def gente_foreign_key(data,column_dic,update_rule_dic,delete_rule_dic):
    foreign_keys=data[1]["object"]["dd_object"]["foreign_keys"]
    fk_create_sql = ""
    for i in foreign_keys:

        update_rule_str = ""
        delete_rule_str = ""
        foreign_key_name=i["name"]
        referenced_table_name=i["referenced_table_name"]
        ##update_rule = i["update_rule"]
        ##print(update_rule)
        ##delete_rule = i["delete_rule"]
        ##print(delete_rule)
        elements = i["elements"]

        ##on delete/on update处理


        ##不处理1 1代表默认 NO ACTION
        ##if i["update_rule"] >1:
        update_rule_str = update_rule_dic.get(i["update_rule"])
            ##print(update_rule_str)

        ##if i["delete_rule"] >1:
        delete_rule_str = delete_rule_dic.get(i["delete_rule"])
            ##print(delete_rule_str)


        ##列处理
        fkey_column_str = ""
        referenced_column_name_str=""
        for j in elements:
            fk_column=column_dic[j["column_opx"]]
            referenced_column_name=j["referenced_column_name"]
            ##print(fk_column)
            ##print(referenced_column_name)
            fkey_column_str = fkey_column_str + column_dic[j["column_opx"]] + ','
            referenced_column_name_str = referenced_column_name_str + referenced_column_name+','
        ##print(fkey_column_str)
        ##print(referenced_column_name_str)
        fk_create_sql_tmp="    CONSTRAINT "+ foreign_key_name + " FOREIGN KEY("+fkey_column_str.strip(",")+") REFERENCES "+referenced_table_name+"("+referenced_column_name_str.strip(",")+") " + delete_rule_str +" "+ update_rule_str
        fk_create_sql=fk_create_sql+fk_create_sql_tmp+",\n"

    return fk_create_sql.strip(",\n")

##字符集
def gente_collation_id(data,character_rule_dic):
    collation_id = data[1]["object"]["dd_object"]["collation_id"]
    return character_rule_dic.get(collation_id,'NONE')

##row_format
def gente_row_format(data,row_format_rule_dic):
    row_format = data[1]["object"]["dd_object"]["row_format"]
    return row_format_rule_dic.get(row_format,'NONE')

##表名
def genete_tablename_sql(data):
    table_name = data[1]["object"]["dd_object"]["name"]
    return table_name
##表备注
def genete_table_comment(data):
    table_comment = data[1]["object"]["dd_object"]["comment"]
    if len(table_comment)>0:
        return "'"+table_comment+"'"
    else:
        return ''

##表引擎
def genete_table_engine(data):
    table_engine = data[1]["object"]["dd_object"]["engine"]
    return table_engine

def genete_column_sql(data):
    columns = data[1]["object"]["dd_object"]["columns"]

    column_sql_str = ""
    for i in columns:
        column_name=i["name"]
        is_nullable=i["is_nullable"]
        is_auto_increment=i["is_auto_increment"]
        char_length = i["char_length"]
        column_type_utf8 = i["column_type_utf8"]
        ##print(column_type_utf8)
        type = i["type"]
        comment=i["comment"]
        default_value= i["default_value_utf8"]
        update_option=i["update_option"]
        hidden=i["hidden"]
        is_zerofill=i["is_zerofill"]
        is_unsigned = i["is_unsigned"]

        if hidden==1:
            # 5 float
            # 6 double real
            # 14 year
            # 15 date
            # 16 varchar varbinary
            # 17 bit
            # 18 timestamp
            # 19 datetime
            # 20 time
            # 21 decimal NUMERIC
            # 22 enum
            # 23 set
            # 24 tinytext tinyblob
            # 25 mediumtext mediumblob
            # 26 longtext longblob
            # 27 text blob
            # 29 char binary
            # 30 point GEOMETRY
            # 31 json
            if type in [5,6,14,15,16,17, 18, 19,20,21,22,23,24, 25, 26, 27,29,30,31]:
                column_type = column_type_utf8


            # 2 tinyint boolean tinyint(1)=boolean
            # 3 smallint
            # 4 int
            ## 9 bigint
            # 10 mediumint

            ##print(is_unsigned)
            ##print(column_type_utf8)
            if type in [2, 3, 4, 9, 10]:
                column_type = column_type_utf8
                ##if (str(is_zerofill)=="True"):
                ##    column_type = column_type_utf8
                ##elif (is_unsigned == "True"):
                ##    column_type = column_type_utf8
                ##else:
                ##    column_type = column_type_utf8 + "(" + str(char_length) + ")"

                ##if (type == 2 ) and (char_length==1):
                ##    column_type = column_type_utf8

            ##print(column_type)
            ##处理默认值
            # 14 year
            # 15 date
            # 17 bit
            # 18 timestamp
            # 19 datetime
            # 20 time
            # 30 point
            # 31 json
            if len(default_value) > 0:
                if type in [14,15,18,17,19,20]:
                    default_value_str = default_value
                elif type in [30,31]:
                    default_value_str = "(" + default_value + ")"
                elif str(default_value).find("uuid_to_bin") >= 0: ##DEFAULT (UUID_TO_BIN(UUID()))
                    default_value_str = default_value_str = "(" + default_value + ")"
                elif str(default_value).find("rand()") >= 0:
                    default_value_str = default_value_str = default_value
                else:
                    default_value_str = "'" + default_value + "'"


            ##是否为空和默认值
            if str(is_nullable)=="False":
                if len(default_value) > 0:
                    is_nullable_str_default_value=" NOT NULL DEFAULT "+ default_value_str
                else:
                    is_nullable_str_default_value = " NOT NULL "
            else:
                if len(default_value) > 0:
                    is_nullable_str_default_value="DEFAULT " + default_value_str
                else:
                    is_nullable_str_default_value = "DEFAULT NULL"

            ##是否自增长
            if str(is_auto_increment)=="True":
                is_auto_increment_str=" AUTO_INCREMENT"
            else:
                is_auto_increment_str = ""

            ##获取comment
            if len(comment)>0:
                comment_str=" COMMENT "+"'"+comment + "'"
            else:
                comment_str = ''

            ##数字类型unsigned处理
            ##if column_type_utf8=="bigint unsigned":
            ##    column_type = "bigint"+"("+str(char_length)+") unsigned"

            ##if column_type_utf8=="int unsigned":
            ##    column_type = "int"+"("+str(char_length)+") unsigned"

            ##if column_type_utf8=="smallint unsigned":
            ##    column_type = "smallint"+"("+str(char_length)+") unsigned"

            ##if column_type_utf8=="tinyint unsigned":
            ##    column_type = "tinyint"+"("+str(char_length)+") unsigned"




            ##update_option
            if len(update_option) > 0:
                update_option_str = " ON UPDATE " + update_option
            else:
                update_option_str = ''

            column_sql=column_name+" "+column_type+" "+is_nullable_str_default_value + str(is_auto_increment_str)+update_option_str+comment_str+","
            ##print(column_sql)
            column_sql_str=column_sql_str+"    "+column_sql+"\n"

    return column_sql_str.strip(",\n")

##列字典
def gente_column_dict(data):
    dict={}
    columns = data[1]["object"]["dd_object"]["columns"]
    for i in columns:
        hidden = i["hidden"]
        if hidden==1:
             dict[i["ordinal_position"]-1] = i["name"]
    return dict

def genete_index_sql(data,column_dic):
    index = data[1]["object"]["dd_object"]["indexes"]
    key_create_sql=""
    for i in index:
        index_name=i["name"]
        index_comment=i["comment"]
        index_type=i["type"]
        ##只有hidden=false的才处理
        if str(i["hidden"]) == 'False':
            ##if index_name=="PRIMARY":
            ##    key_str="PRIMARY KEY"
            ##else:
            ##   key_str = "KEY"


            if index_type==1:
                key_str = "    PRIMARY KEY"

            if index_type==2:
                key_str = "    UNIQUE KEY"

            if index_type==3:
                key_str = "    KEY"

            ##print(key_str)
            if len(index_comment) > 0:
                index_comment_str = " comment " + "'" + index_comment + "'"
            else:
                index_comment_str = ''

            elements=i["elements"]
            ##print(elements)

            key_column_str = ""
            for j in elements:
                ##print(j["hidden"])
                ##print(aa)
                if str(j["hidden"]) == 'False':
                    key_column_str = key_column_str +column_dic[j["column_opx"]]+','

            if index_name == "PRIMARY":
                key_create_sql_tmp=key_str+" ("+ key_column_str.strip(',')+")"+index_comment_str+","
            else:
                key_create_sql_tmp = key_str + " " + index_name + " (" + key_column_str.strip(',') + ")"+index_comment_str+","
            key_create_sql = key_create_sql + key_create_sql_tmp+"\n"

    ##去掉最后一个逗号和换行
    return key_create_sql.strip(",\n")



if __name__ == '__main__':

    ##filename="D:\\jsonfile\\tb_test.txt"
    filename = sys.argv[1]

    with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)
    ##获取表名
    table_name=genete_tablename_sql(data)
    column_dic=gente_column_dict(data)

    ##print(table_name)
    ##获取创建列语句
    column_sql_str = genete_column_sql(data)
    ##print(column_sql_str)

    ##获取表备注
    table_comment=genete_table_comment(data)
    ##print(table_comment)

    ##获取表引擎
    table_engine=genete_table_engine(data)
    ##print(table_engine)

    ##获取字符集 255 utf8mb4 33 utf8 8 latin1
    character_rule_dic=gente_character_rule()
    table_character_name=gente_collation_id(data,character_rule_dic)
    ##print(table_character_name)

    ##索引
    index_sql=genete_index_sql(data,column_dic)

    ##外键
    update_rule_dic = gente_update_rule()
    delete_rule_dic = gente_delete_rule()
    fk_create_sql=gente_foreign_key(data,column_dic,update_rule_dic,delete_rule_dic)

    ##分区
    partion_sql = gente_partition_sql(data)
    ##print(partion_sql)

    ##row_format
    row_format_rule_dic=gente_row_format_rule()
    row_format_str = gente_row_format(data,row_format_rule_dic)


    if len(index_sql)==0:##没有索引
        if len(fk_create_sql)>0: ##有外键
            create_table_sql = 'create table ' + table_name + "(\n" + column_sql_str +fk_create_sql +"\n)"
        else:
            create_table_sql = 'create table ' + table_name + "(\n" + column_sql_str + "\n)"
    else: ##有索引
        if len(fk_create_sql)>0: ##有外键
            create_table_sql='create table '+table_name+"(\n"+column_sql_str+",\n"+index_sql+",\n"+fk_create_sql+"\n)"
        else:
            create_table_sql = 'create table ' + table_name + "(\n" + column_sql_str + ",\n" + index_sql + "\n)"


    ##表备注
    if len(table_comment)>0:
        create_table_sql_last = create_table_sql+ " ENGINE=" +table_engine + " DEFAULT CHARSET="+ table_character_name+" ROW_FORMAT = " + row_format_str + " COMMENT " + table_comment
    else:
        create_table_sql_last = create_table_sql + " ENGINE=" + table_engine + " DEFAULT CHARSET=" + table_character_name + " ROW_FORMAT = " + row_format_str

    ##表分区
    if len(partion_sql) > 0:
        create_table_sql_last = create_table_sql_last + "\n" +partion_sql


    print(create_table_sql_last)

