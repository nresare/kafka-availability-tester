# kafka-availability-tester

# Some thoughts on the definition of available
* A topic is available if a `produce` request returns successfully, and a `consume` response returns the same 
message to the calling application within TIMEOUT seconds.
* Types of failures
    * Produce request returns failure
    * Message is not returned and nothing is returned for longer than TIMEOUT seconds
    * Later messages are returned but a previously sent message appears to be missing
