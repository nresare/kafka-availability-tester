# Files for local testing

These are some files used for local testing that I have used on my Mac. Steps I took.

1. Install kind using homebrew
2. Spin up a kind cluster with three worker nodes, using `kind create cluster --config kind-config.yaml`
3. Set up the https://strimzi.io kafka operator with `kubectl apply -n kafka -f strimzi.yaml`
4. Set up the kafka cluster with `kubectl apply -n kafka -f kafka-cluster.yaml`
5. Set up the services with static pre-configured NodePort port numbers that correspond to the numbers in kind-config
   using `kubectl apply -n kafka -f custom-services.yaml`

You can verify that the port forwarding works by using `kafka-topics.sh --list --bootstrap-server localhost:7070`
(the `kafka-topics.sh` is shipped with the upstream kafka distribution) and do the same for ports 7071 and 7072