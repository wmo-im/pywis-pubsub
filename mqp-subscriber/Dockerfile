###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################

FROM python:3.8-slim-buster

# TO BE ADDED FOR PROXY
# ENV HTTP_PROXY=http://myproxy:port
# ENV HTTPS_PROXY=https://myproxy:port
# ENV NO_PROXY="0.0.0.0,localhost,aria2-pro"
# END ADDED

RUN mkdir -p /usr/src/sub
RUN mkdir -p /usr/src/sub/caFiles
RUN mkdir -p /usr/src/sub/logs
RUN mkdir -p /usr/src/sub/msg_store
RUN mkdir -p /usr/src/sub/org_config
RUN mkdir -p /usr/src/sub/toDownload
RUN mkdir -p /usr/src/sub/toDownload/done

RUN mkdir -p /data/pywis-pubsub/config
RUN mkdir -p /data/pywis-pubsub/config/mqp-subscriber

WORKDIR /usr/src/sub
	
COPY requirements.txt /usr/src/sub/
RUN pip install --no-cache-dir -r /usr/src/sub/requirements.txt
	
COPY pubSubDWD_mqtt_aria2.py /usr/src/sub/
COPY message-schema.json /usr/src/sub/
COPY download.py /usr/src/sub/
COPY start_subscriber.sh /usr/src/sub/
RUN chmod +x /usr/src/sub/start_subscriber.sh
COPY stop_subscriber.sh /usr/src/sub/
RUN chmod +x /usr/src/sub/stop_subscriber.sh
COPY start_py4download.sh /usr/src/sub/
RUN chmod +x /usr/src/sub/start_py4download.sh
COPY stop_py4download.sh /usr/src/sub/
RUN chmod +x /usr/src/sub/stop_py4download.sh
COPY status_py4download.sh /usr/src/sub/
RUN chmod +x /usr/src/sub/status_py4download.sh

COPY configFiles/dwd.txt /data/pywis-pubsub/config/mqp-subscriber/
COPY configFiles/dwd.txt /usr/src/sub/org_config/
COPY configFiles/whitelist.txt /data/pywis-pubsub/config/mqp-subscriber/
COPY configFiles/whitelist.txt /usr/src/sub/org_config/

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

COPY cleanAria2Download.sh /usr/src/sub/
RUN chmod +x /usr/src/sub/cleanAria2Download.sh

ENV PYTHONUNBUFFERED="true"
