#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

usage="Usage: mixserv-daemon.sh (start|stop|status)"

# If no args specified, show usage
if [ $# -ne 1 ]; then
  echo $usage
  exit 1
fi

if [ "$HIVEMALL_HOME" == "" ]; then
  echo env HIVEMALL_HOME not defined
  exit 1
fi

HIVEMALL_PID_DIR=/tmp
HIVEMALL_PID_FILE="$HIVEMALL_PID_DIR/hivemall-$USER.pid"
HIVEMALL_JMXOPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false"
HIVEMALL_VMOPTS="-Xmx4g -da -server -XX:+PrintGCDetails -XX:+UseNUMA -XX:+UseParallelGC"

case $1 in

  (start)
    echo "starting the MIX server"

    # Check if the MIX server has already run
    if [ -f $HIVEMALL_PID_FILE ]; then
      TARGET_ID="$(cat "$HIVEMALL_PID_FILE")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo "the MIX server has already run as process $TARGET_ID"
        exit 0
      fi
    fi

    nohup java ${HIVEMALL_JMXOPTS} ${HIVEMALL_VMOPTS} -jar "$HIVEMALL_HOME/bin/hivemall-fat.jar" > /dev/null 2>&1 &

    newpid="$!"
    echo "$newpid" > "$HIVEMALL_PID_FILE"
    sleep 1

    # Checks if the process has died
    if [[ ! $(ps -p "$newpid" -o comm=) =~ "java" ]]; then
      echo "failed to launch the MIX server"
    fi
    ;;

  (stop)

    if [ -f $HIVEMALL_PID_FILE ]; then
      TARGET_ID="$(cat "$HIVEMALL_PID_FILE")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo "stopping the MIX server"
        kill "$TARGET_ID" && rm -f "$HIVEMALL_PID_FILE"
      else
        echo "no MIX server to stop"
      fi
    else
      echo "no MIX server to stop"
    fi
    ;;

  (status)

    if [ -f $HIVEMALL_PID_FILE ]; then
      TARGET_ID="$(cat "$HIVEMALL_PID_FILE")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo the MIX server is running
        exit 0
      else
        echo file $HIVEMALL_PID_FILE is present but the MIX server is not running
        exit 1
      fi
    else
      echo the MIX server is not running
      exit 2
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac
