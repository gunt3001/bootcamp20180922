# Shopee Bootcamp 2018

### Setup
Connect to hadoop cluster

* `path_to_your_key` is the full path of your private key received before the bootcamp.
* `username` is your email prefix.

```bash
$ chmod 600 <path_to_your_key>

# please choose any one of command, we had two clusters.
$ ssh -i <path_to_your_key> hadoop@ec2-52-221-226-112.ap-southeast-1.compute.amazonaws.com
$ ssh -i <path_to_your_key> hadoop@ec2-54-169-163-25.ap-southeast-1.compute.amazonaws.com
```

Open your spark shell
```bash
$ pyspark
```

If you see this output in your terminal
```bash
Python 2.7.14 (default, May  2 2018, 18:31:34) 
[GCC 4.8.5 20150623 (Red Hat 4.8.5-11)] on linux2
Type "help", "copyright", "credits" or "license" for more information.
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
18/09/21 08:22:53 WARN Client: Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.3.1
      /_/

Using Python version 2.7.14 (default, May  2 2018 18:31:34)
SparkSession available as 'spark'.
>>> 
```

Good, all done. Please wait for instructor to start the session.
