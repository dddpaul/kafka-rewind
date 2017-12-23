# kafka-rewind

A tool for Kafka consumer offset rewind


    

```
Usage: docker run dddpaul/kafka-rewind [-ch] -g=<groupId> [-k=<keyDeserializer>] [-p=<timeout>]
                    [-s=<servers>] -t=<topic> [-v=<valueDeserializer>]
                    -o=<String=LocalDate> [-o=<String=LocalDate>]...
  -c, --consume               Consume after seek
  -g, --group-id=<groupId>    Consumer group ID
  -h, --help                  Display a help message
  -k, --key-deserializer=<keyDeserializer>
                              Consumer key deserializer
  -o, --offset=<String=LocalDate>
                              Partition to offset map
  -p, --poll-timeout=<timeout>
                              Consumer poll timeout, ms
  -s, --servers=<servers>     Comma-delimited list of Kafka brokers
  -t, --topic=<topic>         Topic name
  -v, --value-deserializer=<valueDeserializer>
                              Consumer value deserializer
```

For example:

```
docker run dddpaul/kafka-rewind --servers=kafka:9092 --group-id=id1 --topic=topic --o 0=2017-12-21 --o 2017-12-21 --o 2=2017-12-21 --consume
```
