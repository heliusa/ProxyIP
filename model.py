# -*- coding: utf-8 -*-

from peewee import *
from config import settings
import datetime
from util.log import Log
from ProxyIP import ProxyItem
import json

logging = Log()

if settings.DBENGINE.lower() == 'mysql':
    database = MySQLDatabase(
        settings.DBNAME,
        host=settings.DBHOST,
        port=settings.DBPORT,
        user=settings.DBUSER,
        passwd=settings.DBPASSWORD,
        charset='utf8',
        use_unicode=True,
    )
elif settings.DBENGINE.lower() == 'sqlite3':
    database = SqliteDatabase(settings.DBNAME)

elif settings.DBENGINE.lower() == 'postgresql':
    database = PostgresqlDatabase(
        settings.DBNAME,
        user=settings.DBUSER,
        password=settings.DBPASSWORD,
        host=settings.DBHOST,
        charset='utf8',
        use_unicode=True,
    )

else:
    raise AttributeError("Please setup datatbase at settings.py")


class BaseModel(Model):
    class Meta:
        database = database

class ProxyIp(BaseModel):
    ip = CharField(max_length=128) 
    port =  IntegerField()
    address = CharField(max_length=160) # 服务器地址
    channel = CharField(max_length=64) # 渠道：default, xici, 
    type = CharField(max_length=64) # 渠道：default, xici, 
    protocol = CharField(max_length=64, default='UNKNOWN') #协议：http/https/socket5
    country = CharField(max_length=64, null=True) #国家: 中国
    status = IntegerField(default=0) # 状态: 1: 有效, 0: 未知；-1: 删除；2：无效
    speed = IntegerField(null=True) #速度， 单位：毫秒
    timeout = IntegerField(null=True) #连接时间, 单位：毫秒
    alive_first = DateTimeField(null=True) #第一次存活时间
    alive_near = DateTimeField(null=True) #最近一次存活时间
    die_time = DateTimeField(null=True)
    created_at = DateTimeField(default=datetime.datetime.now)
    validdate = DateTimeField(default=datetime.datetime.now)
    class Meta:
        primary_key = CompositeKey('ip', 'port')
        table_name = 'proxy_ip'


def database_init():
    database.connect()
    database.create_tables(
        [ProxyIp], safe=True)
    database.close()

if __name__ == "__main__":
    database_init()
   
    # url_xici = [
    #         ProxyItem('xici', "http://www.xicidaili.com/nn/%d" % (index + 1))
    #         for index in range(3275)
    #     ]

    # logging.info(json.dumps(url_xici, default=lambda obj: obj.__dict__, sort_keys=True, indent=4))