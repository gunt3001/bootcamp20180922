spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1 \
    --py-files lib.zip \
    MyStreamingETL.py