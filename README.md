# kafka-rewind
Kafka consumer offset rewinder


```
docker run dddpaul/kafka-rewind --servers=kafka:9092 --id=id1 --topic=topic --offset=0:2017-12-21 --offset=1:2017-12-21 --offset=2:2017-12-21 --consume
kafkacat -b kafka:9092 -e -G id1 topic
```
