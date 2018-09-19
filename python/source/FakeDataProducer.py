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

import datetime
import json
import time

from random import random, randint
from kafka import KafkaProducer
from conf.Kafka import config

def produce_mock_orders_json(brokers, topic):
    """
    Fake data are real time order events produced into kafka, and data is json format 
    with fixed schema follow:
    
    itemid: a unique id of each single item of each single shop.
    price: total price of a single order.
    shopid: a unique id of each single shop.
    count: number of items bought in each single order.
    timestamp: event timestamp when each order placed.
    region: region of each order placed.
    orderid: a unqiue id of each single order.
    """
    producer = KafkaProducer(bootstrap_servers=brokers.split(","), retries=5)
    shopid = [id for id in xrange(100000, 101200)]
    itemid = [(id, round(random() * randint(2, 100), 2), shopid[id % 1200]) for id in xrange(100000, 130000)]
    orderid = 1000000000000
    region = ['SG', 'VN', 'TW','ID', 'TH','TW','ID','ID', 'TW','TW','TW','ID','TW', 'TH', 'MY', 'PH', 'ID']
    print "Start produce data into kafka %s, topic %s ..."  % (brokers, topic)
    while True:
        item = itemid[randint(0, len(itemid)-1)]
        record = {
            "itemid": item[0],
            "price": item[1],
            "shopid": item[2],
            "count": randint(1, 100),
            "timestamp": int(time.mktime(datetime.datetime.now().timetuple())),
            "region": region[randint(0,len(region)-1)],
            "orderid": orderid
        }
        print record
        orderid += 1
        producer.send(topic, json.dumps(record))
        time.sleep(randint(50, 600) / 1000.0)

if __name__ == '__main__':
    produce_mock_orders_json(
        config.get('brokers', ''), 
        config.get('topic', '')
    )
    