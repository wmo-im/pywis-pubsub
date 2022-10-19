#!/bin/sh
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

set +e
DIR="/usr/src/sub"
if [ -d "$DIR" ]; then

  CLEANJOB_FILE="/usr/src/sub/cleanAria2Download.sh"
  if [ -f "$CLEANJOB_FILE" ]; then
    /usr/src/sub/cleanAria2Download.sh > /usr/src/sub/logs/clean.log &
    echo "INFO - STARTED: cleanAria2Download.sh"
  else
    echo "ERROR - $CLEANJOB_FILE missing"
  fi
  

  DIR2="/data/pywis-pubsub/config/mqp-subscriber/"
  if [ -d "$DIR2" ]; then
    SUB_FILE="/usr/src/sub/pubSubDWD_mqtt_aria2.py"
    if [ -f "$SUB_FILE" ]; then
      echo "INFO - SUB WITH FILE: $SUB_FILE"
    
      CONFIG_FILE="/data/pywis-pubsub/config/mqp-subscriber/dwd.txt"
      WHITELIST_FILE="/data/pywis-pubsub/config/mqp-subscriber/whitelist.txt"
      if [ -f "$CONFIG_FILE" ]; then
          echo "INFO - USED LOCAL FILE VERSION: $CONFIG_FILE"
      else
          cp /usr/src/sub/org_config/dwd.txt /data/pywis-pubsub/config/mqp-subscriber/
          echo "INFO - COPIED FILE: $CONFIG_FILE to /data/pywis-pubsub/config/mqp-subscriber/"
      fi

      if [ -f "$WHITELIST_FILE" ]; then
          echo "INFO - USED LOCAL FILE VERSION: $WHITELIST_FILE"
      else
          cp /usr/src/sub/org_config/whitelist.txt /data/pywis-pubsub/config/mqp-subscriber/
          echo "INFO - COPIED FILE: $WHITELIST_FILE to /data/pywis-pubsub/config/mqp-subscriber/"
      fi
      /usr/src/sub/start_subscriber.sh
    else
      echo "ERROR - $SUB_FILE missing"
    fi
  else
    echo "ERROR - $DIR2 missing"
  fi
else
  echo "ERROR - $DIR missing"
fi

# check process running
PID_FILE="/usr/src/sub/pid_sub.txt"
PID=$(cat $PID_FILE)
if ! kill -0 $PID 2>/dev/null;
then
  echo "ERROR - subscriber with (pid: $PID) not running, exit container"
  SUB_RUNNING=false
else
  SUB_RUNNING=true
fi

while $SUB_RUNNING
do
  if ! kill -0 $PID 2>/dev/null;
  then
    echo "ERROR - subscriber with (pid: $PID) not running, exit container"
    SUB_RUNNING=false
  else
    sleep 10
  fi
done
