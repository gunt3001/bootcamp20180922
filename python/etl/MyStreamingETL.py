# -*- coding: utf-8 -*-
#
# MIT License

# Copyright (c) 2018 Shopee Singapore Pte Ltd

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import TopicAndPartition

from pyspark.sql.functions import window, col, sum, count

"""
configurations
"""
# fields we need from data source.
# since our goal is to aggregate gross income (sum(price)), 
# so we just aggregation dimensions -> region, shopid, itemid, timestamp,
# and value field is -> price. 
schema = ['region', 'shopid', 'itemid', 'timestamp', 'price']

# fields needed in output data
# we need to change some of columns' names to make it more ananlytics friendly.
final_schema = ['region', 'shopid', 'itemid', 'timestamp', 'gross_income']

# all configurations are here
# our spark app name
spark_app_name = "my streaming etl"
kafka_brokers = "172.31.0.84:9092"
kafka_topic = "workshop-test"
mysql_endpoint = "shopee-de-workshop.cpbs4qr3yd7i.ap-southeast-1.rds.amazonaws.com"
mysql_username = "workshop"
mysql_password = "ShopEE123"
mysql_database = "workshop"
mysql_table = "agg_order_minute_tab"
# this is to indicate aggregate time window.
# unit is second
aggregation_interval = 5
    
def formReducerKeyAndValue(key_value_pair):
    """
    input: {"itemid": 125717, "timestamp": 1537514430, "price": 4.03, "shopid": 100917, "region": "TW"}
    output: (TW-100917-125717-1537514430 / interval * interval, 4.03) 
    
    please be noted: timestamp need to be rounded to match the time window.
    for example: 
        your aggregation interval is 10s, then
        2018-09-22 10:25:33 needs to be 2018-09-22 10:25:30
        
        so that we could use 2018-09-22 10:25:30 to indicate time window 
        since 2018-09-22 10:25:30 to 2018-09-22 10:25:40
    """
    return (
        "{0}-{1}-{2}-{3}".format(
            key_value_pair['region'],
            key_value_pair['shopid'],
            key_value_pair['itemid'],
            key_value_pair['timestamp'] / aggregation_interval * aggregation_interval
        ),
        key_value_pair['price']
    )
    
def processReducedResult(key_value_tuple):
    """
    input: (TW-100917-125717-1537514430, 4.03) 
    output: [TW, 100917, 125717, 1537514430, 4.03]
    """
    values = key_value_tuple[0].split("-")
    values.append(str(key_value_tuple[1]))
    
    return values
    
def insertResultToMySQL(iter):
    import sys
    sys.path.insert(0, 'lib.zip')
    import mysql.connector
    conn = None
    cursor = None
    conn = mysql.connector.connect(host=mysql_endpoint, user=mysql_username, password=mysql_password, database=mysql_database)
    cursor = conn.cursor()
    for statement in iter:
        cursor.execute(statement)
    conn.commit()
    cursor.close()
    conn.close()

def formSQLStatement(values):
    """
    To send sql statements to mysql database.
    """
    global final_schema
    
    return "REPLACE INTO {0} ({1}) VALUES ({2});".format(
        mysql_table,
        ','.join(final_schema), 
        ','.join("'{0}'".format(value) for value in values)
    )

def processRDD(batch_time, rdd):
    """
    This part of code is to process the streaming data.
    As the goal of this workshop, we need to calculate gross income of 
    each item from each shop of each time window.
    """
    global schema
    
    # input rdd is similar to a list containing a tuple 
    # like (None, u'{"itemid": 113451, "count": 23, "timestamp": 1537501185, "price": 1.52, "orderid": 1000000155691, "shopid": 100651, "region": "TW"}')
    # so we pick up the second element,
    # load each input string as a dict.
    all_strings_are_dict_rdd = rdd.map(lambda x: json.loads(x[1]))
    
    # filter out those fields needed only.
    # we defined this in global variable 'schema'.
    pick_up_fields_rdd = all_strings_are_dict_rdd.map(lambda x: {k: v for (k, v) in x.items() if k in schema}) 

    # since we want to calculate sum(price) based on 
    # region, shopid, itemid, timestamp,
    # we need to firstly form a tuple of key and value.
    # here we try to concatenate region, shopid, itemid, timestamp as a 
    # single string to be the key, and price to be the value
    # to get something like:
    #   
    #   (SG-1029983-120093-1564728192, 25.5)

    key_value_rdd = pick_up_fields_rdd.map(formReducerKeyAndValue) 

    # we try to combine those tuple with the same key
    # reduceByKey will shuffle the tuple with the same key into the
    # same executor, and execute func we provided, here is 
    # lambda x, y: x + y. 
    # x and y denote values of each two tuples with the same key. 
    total_price_rdd = key_value_rdd.reduceByKey(lambda x, y: x + y)

    # after procedures above, we got a list of aggregated result
    # looks like:
    #
    # (SG-1023123-120656-1564728192, 25.5),
    # (SG-1029983-120854-1564728192, 1.7),
    # (SG-1025544-122333-1564728192, 89.5)
    # 
    # so we try to split the key back into separate fields to
    # prepare for SQL statement.
    finalized_rdd = total_price_rdd.map(processReducedResult)

    # we generate SQL Statements according to fields we aggregate as 
    # well as fields we want to output. 
    sql_statements_rdd = finalized_rdd.map(formSQLStatement)

    # we return final RDD with a list of SQL statements. 
    return sql_statements_rdd


def run(kafka_brokers, kafka_topics, mysql_endpoint, mysql_username, mysql_password):
    """
    Start the program here. 
    """
    sc = SparkContext("yarn", "my streaming etl")
    ssc = StreamingContext(sc, aggregation_interval)
    KafkaUtils.createDirectStream(
        ssc,
        topics=[kafka_topics, ],
        kafkaParams = {
            "bootstrap.servers": kafka_brokers,
            "auto.offset.reset": "largest"
        }
    ) \
    .transform(processRDD) \
    .foreachRDD(lambda rdd: rdd.foreachPartition(insertResultToMySQL))
    
    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    run(kafka_brokers, kafka_topic, mysql_endpoint, mysql_username, mysql_password)
    