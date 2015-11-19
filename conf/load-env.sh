#!/usr/bin/env bash

HIVEMALL_JMXOPTS+="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false"
HIVEMALL_VMOPTS+="-Xmx4g -da -server -XX:+PrintGCDetails -XX:+UseNUMA -XX:+UseParallelGC"
HIVEMALL_OPS+="-sync 30"

