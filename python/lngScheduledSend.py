# -*- coding: UTF-8 -*-
import pandas as pd
import time
from kafka import KafkaProducer
from kafka.errors import kafka_errors
import traceback
import json

def processCsv(csvPath):
    #  读取文件，并按照时间戳排序
    aisData = pd.read_csv(csvPath, )
    aisData = aisData.sort_values(by="timestamp")
    # 计算与首个时间戳的插值
    aisData.loc[:, "index"] = aisData["timestamp"] - aisData.iloc[0, 1]
    return (aisData)

def scheduledSending():
    aisData = processCsv("sea.csv") # 此处需指定csv文件所在位置

    # 配置kafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=['192.168.94.137:9092'], # 指定kafka集群所在IP及端口
        key_serializer=lambda k: json.dumps(k).encode(),
        value_serializer=lambda v: json.dumps(v).encode(),

    )
    # 获取当前时间
    timeStart = time.perf_counter()

    for row in aisData.iterrows():
        timeNow = time.perf_counter()
        # 计算时间间隔
        while int((timeNow - timeStart) * 1000) < int(row[1]["index"]):

            time.sleep(1)
            timeNow = time.perf_counter()

            if int((timeNow - timeStart) * 1000) >= int(row[1]["index"]):
                break
        # 按照数据的时间间隔发送
        future = producer.send(
            'pythonTest',

            value=row[1][0:-1].to_json(),

        )
        try:
            future.get(timeout=10)  # 监控是否发送成功
        except kafka_errors:  # 发送失败抛出kafka_errors
            traceback.format_exc()
        print(row[1][0:-1].to_json())

if __name__ == '__main__':
    scheduledSending()




