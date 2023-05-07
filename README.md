# kafka-availability-tester

kafka-availability-tester is a small self-contained go application that tests the availability of a
Kafka cluster by connecting to it with a producer and consumer and measures the round-trip time for 
a sequence of messages.

If your Kafka server is not set up to create topics on the fly, you may need to create the test topic
before using.

# Authentication with Azure AD

Azure AD provides tokens that can be used to authenticate with Kafka. The steps I took to test this:
1. Generate a cert and key pair with `openssl req -new -newkey rsa:4096 -nodes -x509 -subj "/CN=kafka-client" -keyout key.pem -out cert.pem`
2. Create an App under App registrations in the Azure portal 
3. Upload the cert there
4. Copy the file `azure-config-template.toml` and fill out the values needed to obtain a token 
5. If your kafka cluster's tls listeners does not provide a cert signed by widely trusted CA, connect
   using `openssl s_client -showcerts -connect remote:port` and copy the root CA cert to a file `ca-cert.pem`
6. Invoke kafka-availability-tester with the following parameters 
   `--authentication oauthbearer --ca-cert-path ca-certs.pem --azure-token-config-path AZURE_CONFIG.toml`
   referencing the file from 4 above 

# TODO
* Support producing to and consuming from multiple partitions
* Making statistics available in Prometheus format
* If there is a failure before the consumer successfully joins the security group, ctl-c does not end execution

# The definition of 'available'
* A topic is available if a `produce` request returns successfully, and a `consume` response returns the same 
message to the calling application within TIMEOUT seconds.
* Types of failures
    * Produce request returns failure
    * Message is not returned and nothing is returned for longer than TIMEOUT seconds
    * Later messages are returned but a previously sent message appears to be missing
