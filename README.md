# NIFI PubsubPublisher Processor

This is an [Apache NIFI](https://nifi.apache.org/) processor that uploads messages to Google Cloud Platform (GCP) PubSub topic. The operation is quite simple, it just needs to know the name of topic, project ID and the authentication keys if it is running outside a GCP compute instance.

## Installation
* mvn package
* cp ./target/*.nar $NIFI_HOME/libs
