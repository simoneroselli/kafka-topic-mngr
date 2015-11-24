# kafka-topic-mngr
## Keep Kafka topics configuration static

### Output example:
```
./kafka-topic-mngr
Creating new topic "example-topic1 --replication 2 --partitions 3" ..
Created topic "example-topic1"!
Topic 'example-topic1' configured with {'index.interval.bytes': '496'}
Topic 'example-topic1' configured with {'retention.ms': '2000000'}
Topic 'example-topic1' configured with {'segment.index.bytes': '10'}
Topic 'example-topic1' configured with {'segment.bytes': '2'}
```
