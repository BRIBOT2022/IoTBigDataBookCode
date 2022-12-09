# -*- coding: UTF-8 -*-
from kafka import KafkaConsumer
import json
import pymysql

def consumer_demo():
    # 配置kafka连接
    consumer = KafkaConsumer(
        'python_test',# 指定Kafka的Topic
        bootstrap_servers='192.168.10.137:9092',# 此处为Kafka集群所在端口，											#输入集群之一的IP及端口即可
        group_id='test',# 消费组名，可以自定义
        auto_offset_reset='latest' # 从最新的消息开始消费
    )
    # 获得kafka消息队列中的消息
    for message in consumer:
        # 加载为字典类型
        AIS = str(message.value.decode())
    # 因为字符串可是问题需要使用json加载两次
        AisDict = json.loads(json.loads(AIS))
        # 使用函数插入进mysql中
        insertIntoMysql(AisDict)


def insertIntoMysql(AisDict):
    # 配置数据库连接字符串
    db = pymysql.connect(
        host='localhost', # 数据库所在IP地址 ，若不是默认端口号，需指定端口
        user='root', # 管理员用户名
        password='123456', # 自己设置的密码
        database='kafkatest' # 数据库名
    )
    # 编写sql语句
    sql = "INSERT INTO aisdata \
           VALUES (%s, %s,  %s,  %s,  %s,%s)" % \
          (AisDict["roadId"], AisDict["timestamp"], AisDict["longitude"],
           AisDict["latitude"], AisDict["directionAngle"], AisDict["type"])

    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    # 执行sql语句
    cursor.execute(sql)
    # 提交结果
    db.commit()
    # 关闭数据库连接
    db.close()

if __name__ == '__main__':
    consumer_demo()

