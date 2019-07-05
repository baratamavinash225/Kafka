kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic cpu_usage_metric

kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic cpu_metric_sum

kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic cpu_metric_average
