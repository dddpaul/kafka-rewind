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
  -o, --offset=<String=LocalDateTime>
                              Partition to timestamp map
  -p, --poll-timeout=<timeout>
                              Consumer poll timeout, ms
  -s, --servers=<servers>     Comma-delimited list of Kafka brokers
  -t, --topic=<topic>         Topic name
  -v, --value-deserializer=<valueDeserializer>
                              Consumer value deserializer
```

For example:
```
docker run dddpaul/kafka-rewind --servers=kafka:9092 --group-id=id1 --topic=topic -o 0=2017-12-01 -o 1=2017-12-01 -o 2=2017-12-01
```

The same could be done with Kafka bundled `kafka-consumer-groups.sh`:
```
docker run wurstmeister/kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --topic topic:0,1,2 --group id1 --reset-offsets --to-datetime "2017-12-01T00:00:00.000" --execute
```

The difference is that `kafka-rewind` tool can be used to seek to different timestamps per partitions, but it's a rare case though.
