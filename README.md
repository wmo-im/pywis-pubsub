# pywis-pubsub
WIS2 downloader

# General information
Docker to subscribe to a message broker and download data from URL included in message via aria2 or py-downloader

# Build
python3 pywis-pubsub-ctl.py build

# Start container
python3 pywis-pubsub-ctl.py start

# Configuration for sub and download
config files for sub and download are under mqp-subscriber/configFiles
- dwd.txt
- whitelist.txt

config field | default | description | example
-------------|---------|-------------|--------
"wis2box" | "False" | Use py-scripts inside a docker container or standalone | "True" (if inside docker container)
"toSubscribe" | "False" | Subscribe to a message broker (must be set to "True") | "True"
"sub_host" | NO default | Hostname of the message broker you want to subscribe to | "oflkd011.dwd.de"
"sub_port" | NO default | Port of the message broker you want to subscribe to | "8883"
"sub_cacert" | "/usr/src/sub/caFiles/ca-bundle.crt" (for docker container) | path/to/cacert_file.crt
"sub_protocol" | No default | MQP Protocol to use for subscrption (should be "mqtts", amqp(s) not supported) | "mqtts"
"sub_protocol_version" | "MQTTv5" | MQTT protocol version 5 or 3.1.1 | "MQTTv5"
"sub_user" | NO default | User to authenticate for subscription |
"sub_password" | NO default | Password for subscription |
"sub_clientname" | hostname (if value set in config file, hostname_valueSetInConfig) | clientname | "wis2box_mqp-subscriber"
"sub_topic" | ['#'] | topics to subscribe to | ["cache/v04/#"]
"sub_logfile" | "sub_connect_" + printTimeNow + ".log" | name for logfile | "/usr/src/sub/logs/dwd.log"
"sub_loglevel" | "INFO" | leg level | "INFO"
"sub_maxMSGsize" | 2048 | max allowed message size | 2048
"sub_share_name" | "" (must be changed, use a unique groupname for each shared subscription) | MQTTv5 supports shared subscriptions, groupname for all clients sharing a subscription | "wis2box_mygroupname" (change to own uinque groupname)
"sub_share_quantity" | 1 | Number of clients per topic for shared subscriptions | 5 
"show_message" | "False" | print messages to stdout? | "False"
"msg_store" | None (msg_store is needed for downloader) | directory for message store (write messages with data_id as files) | "/usr/src/sub/msg_store/"




to be continued
