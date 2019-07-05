<u>Finding the Average of the cpu_usage from the IoT Devices.</u>

1. Start the confluent Hub Kafka via docker.
2. create the 3 topics cpu_usage_metric, cpu_metric_sum, cpu_metric_average
3. Post the raw messages onto the kafka topic cpu_usage_metric via kafka rest proxy.
4. Read the message from KStream, convert it to JSON object,perform average on the necessary key from the Json
5. Write to 2 topics, one with sum and other with average.
