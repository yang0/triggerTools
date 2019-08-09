import pymysql
from string import Template

#TABLES = "account_consumption,account_rechargerecordaccount_consumption,account_rechargerecord,agreement_enterpriseexchange,agreement_enterpriseexchangehistory,agreement_medicalexchange,agreement_medicalexchangehistory,agreement_productexchange,agreement_productexchangehistory,enterprise_material,product_material,medical_material,record_operation,record_wechatregisterorder,user,enterprise"

TABLES = "account_consumption, drug"

SOURCE_DB = "wangqian"
TARGET_DB = "bigdata"
PREFIX = "wq_"

# 从a表复制数据到b表，全量复制
DUMP_SQL = """
use ${sourcedb};
insert into ${targetdb}.${prefix}${table} select * from ${table};
"""

#insert 触发器
INSERT_SQL = """
DELIMITER $$
USE `${sourcedb}`$$
DROP TRIGGER IF EXISTS `ins_${table}` $$
create trigger `ins_${table}` 
after insert on `${table}` 
for each row 
begin
      insert into ${targetdb}.${prefix}${table}
      select * from ${table} where id=new.id;
end $$
DELIMITER ;
"""

#delete 触发器
DELETE_SQL = """
DELIMITER $$
USE `${sourcedb}`$$
DROP TRIGGER IF EXISTS `del_${table}` $$
create trigger `del_${table}` 
after delete on `${table}` 
for each row 
begin  
      delete from ${targetdb}.${prefix}${table}
      where id=old.id;
end $$
DELIMITER ;
"""

#update 触发器
UPDATE_SQL = """
DELIMITER $$
USE `${sourcedb}`$$
DROP TRIGGER IF EXISTS `update_${table}` $$
create trigger `update_${table}` 
after update on `${table}` 
for each row 
begin  
      update ${targetdb}.${prefix}${table}
      set ${updateKV} where id=new.id;
end $$
DELIMITER ;
"""


def getTableInfo(tables):
    tableList = tables.split(",")
    tableList = [table.strip() for table in tableList]
    # 打开数据库连接
    db = pymysql.connect("localhost","root","abc123", "wangqian" )
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    tableInfo = {}
    for table in tableList:
        cursor.execute("SHOW columns FROM "+table)
        columes = [column[0] for column in cursor.fetchall()]
        tableInfo[table] = columes
    db.close()

    return tableInfo

class TriggerMaker:

    def __init__(self):
        self.kv = {}
        self.kv["sourcedb"] = SOURCE_DB
        self.kv["targetdb"] = TARGET_DB
        self.kv["prefix"] = PREFIX
        templateStr = DUMP_SQL + INSERT_SQL + UPDATE_SQL + DELETE_SQL
        self.template = Template(templateStr)


    def make(self, tables):
        tableInfo = getTableInfo(tables)
        
        with open("./trigger.sql","w") as f:
            for table in tableInfo:
                self.kv["table"] = table
                kvMap = map(lambda x: x+"=new."+ x, tableInfo[table])
                updateKVString = ", ".join(list(kvMap))
                self.kv["updateKV"] = updateKVString
                s= self.template.substitute(self.kv)

                f.write(s)

        print("sql脚本保存到当前目录了：trigger.sql")


if __name__ == '__main__':
    TriggerMaker().make(TABLES)

