# kafka-topic-mngr
## Keep Kafka topics configuration static

### Output example:
<code>
./kafka-topic-mngr
Creating new topic "example-topic1 --replication 1 --partitions 1" ..
Created topic "example-topic1".
Topic 'example-topic1' configured with {'index.interval.bytes': '496'}
Topic 'example-topic1' configured with {'retention.ms': '2000000'}
Topic 'example-topic1' configured with {'segment.index.bytes': '10'}
Topic 'example-topic1' configured with {'segment.bytes': '2'}
<code>
