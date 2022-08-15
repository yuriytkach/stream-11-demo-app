# Agenda for the online stream

1. Kafka structure - brokers, zookeeper, producers, consumers
2. Topic structure - partitions, consumer offsets, replication
3. Producer
   - Simple send - with key, with custom partition
   - JsonSerializer
4. Consumer
   - Simple receive - ConsumerRecord, Message, @Payload, Object, poll_interval
   - JsonDeserializer
   - Batch consumer - max_records, idleBetweenPolls
   - Error handler - DLT, BackOff
5. Integration Test
