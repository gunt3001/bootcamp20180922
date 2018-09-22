# My Streaming ETL

This module is a streaming etl that consumes data from a kafka cluster, and aggregates data every certain interval and write aggregated results to a mysql database.

### Background
![ETL Structure](data-flow.png)

##### Input Data Format

each record is a json data contains region, shopid, itemid, count, timestamp, price, orderid.

```json
{"itemid": 125717, "count": 83, "timestamp": 1537514430, "price": 4.03, "orderid": 1000000196283, "shopid": 100917, "region": "TW"}
{"itemid": 122490, "count": 55, "timestamp": 1537514431, "price": 24.75, "orderid": 1000000196284, "shopid": 100090, "region": "TW"}
{"itemid": 109561, "count": 38, "timestamp": 1537514431, "price": 28.05, "orderid": 1000000196285, "shopid": 100361, "region": "MY"}
```

##### Output Data Format
what we want to get is aggregated data which shows in the past certain interval, how much gross income that a shop had made from a single item.
```mysql
+-----+--------+--------+--------+------------+--------------+---------------------+
| id  | region | shopid | itemid | timestamp  | gross_income | utime               |
+-----+--------+--------+--------+------------+--------------+---------------------+
|   1 | SG     | 101172 | 119972 | 1537507860 |           43 | 2018-09-21 06:17:46 |
|   2 | TW     | 100527 | 115727 | 1537513980 |            9 | 2018-09-21 07:13:12 |
|   3 | SG     | 100218 | 121418 | 1537513980 |           29 | 2018-09-21 07:13:12 |
|   4 | SG     | 100336 | 107136 | 1537513980 |            6 | 2018-09-21 07:13:12 |
|   5 | TW     | 100227 | 111827 | 1537513980 |           23 | 2018-09-21 07:13:12 |
|   6 | TW     | 101165 | 105565 | 1537513980 |           52 | 2018-09-21 07:13:12 |
|   7 | TW     | 100880 | 106480 | 1537513980 |           64 | 2018-09-21 07:13:12 |
|   8 | TH     | 101014 | 121014 | 1537513980 |           14 | 2018-09-21 07:13:16 |
|   9 | ID     | 100818 | 104018 | 1537513980 |           19 | 2018-09-21 07:13:16 |
|  10 | TW     | 100502 | 124102 | 1537513980 |           25 | 2018-09-21 07:13:16 |
+-----+--------+--------+--------+------------+--------------+---------------------+
```

### Coding Instruction
In this section, we will mainly focus on directory `bootcamp20180922/python/etl`. Here is where our spark program locates.

##### Folder Structure
* `MyStreamingETL.py` this file is our spark program to implement functionalities we described in previous section.
* `makefile` this is to pack dependencies into a zip file so that we could submit it along with spark program to make sure each executor is able to import these dependencies.
* `requirements.txt` this file describes what python dependencies we would like to use in this spark program. In this workshop, we only need `mysql-connector-python` only.
* `start-etl.sh` this bash is to submit our spark program to cluster.

##### Programming Guide
Let's open `MyStreamingETL.py`. 

Main logic is included in function 

```python
def processRDD(batch_time, rdd)
```

Your tasks are to implement two functions

```python
def formReducerKeyAndValue(key_value_pair)
```

and 

```python
total_price_rdd = key_value_rdd.reduceByKey(lambda x, y: pass)
```

Please follow the instruction in comment to finish your code.

Finally, please look for mysql table name we created for you at `Superset`
http://superset.lumos.idata.shopee.com/superset/sqllab 

Steps to mysql table name:
* Log in with user name: bootcamp, password: bootcamp
* Click SQL Lab tab in the header
* Specify Database to `workshop_db`
* Specify Schema to `workshop_db`
* type your name in `Add a table`, auto completion will help you find table name with your name.

Specify code line 56 in `MyStreamingETL.py` to table name you found above.

##### Excute Your Program
Put your code onto server

```
cd bootcamp20180922/python

scp -i <path_to_your_key> -r etl hadoop@ec2-52-221-226-112.ap-southeast-1.compute.amazonaws.com:<your own folder name>
scp -i <path_to_your_key> -r etl hadoop@ec2-54-169-163-25.ap-southeast-1.compute.amazonaws.com:<your own folder name>
```
Please note the colon at the end of command. 

Log in our cluster again, and open directory `etl` and build your project

```bash
cd etl

make build
```

Run your code in Spark
```bash
bash start-etl.sh
```

After a while, you could be able to view the result from `Superset`
http://superset.lumos.idata.shopee.com/superset/sqllab 

### Got Problem?
No worries, please checkout branch to `workshop`, you will get full
program provided by us, and test it to get an intuition of spark streaming.

```bash
git fetch

git checkout workshop
```
