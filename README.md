# kafka-availability-tester

kafka-availability-tester is a small self contained go application that tests the availability of a
Kafka cluster by connecting to it with a producer and consumer and measures the round-trip time for 
a sequence of messages.

# TODO

* Support producing to and consuming from multiple partitions
* Authentication
* Making statistics available in Prometheus format


# The definition of 'available'
* A topic is available if a `produce` request returns successfully, and a `consume` response returns the same 
message to the calling application within TIMEOUT seconds.
* Types of failures
    * Produce request returns failure
    * Message is not returned and nothing is returned for longer than TIMEOUT seconds
    * Later messages are returned but a previously sent message appears to be missing
