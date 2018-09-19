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

from pyspark.sql.function import window, col, sum, count

class MyStreamingETL(object): 

    def __init__(self, brokers, topic, jdbcUrl, user, pwd):
        self.brokers = brokers
        self.topic = topic
        self.jdbcUrl = jdbcUrl
        self.user = user
        self.pwd = pwd

    def run(self, brokers, topic):
        df = spark \
            .readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', self.brokers) \
            .option('subscribe', self.topic) \
            .load()
            .selectExpr('CAST(key AS STRING)', 'CAST(value AS STRING)') \
            .map(lambda x: json.loads(x)) \
            .groupBy(
                window('timestamp', '1 minute'),
                'shopid',
                'itemid',
                'region' 
            ) \
            .agg(
                col('timestamp').alias('event_time'),
                sum('price').alias('gross_income'),
                sum('count').alias('total_items_sold'),
                count('orderid').alias('total_orders_sold')
            )\
            .select(
                'region',
                'shopid',
                'itemid',
                'event_time',
                'gross_income',
                'total_items_sold',
                'total_orders_sold'
            )\
            .writeStream \
            .format('console') \
            .start()
        
