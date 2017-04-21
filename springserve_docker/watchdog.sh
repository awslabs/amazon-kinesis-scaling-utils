#!/bin/bash
#set -x

if [ "$1" == "" ]; then
  echo "Must provide a configuration file which defines the Autoscaling Rules"
  exit -1
fi

while (true); do
  controller_running=`ps -eaf | grep "AutoscalingController" | grep -v "grep" | wc -l | awk '{print $1}'`

  if [ "$controller_running" -eq "0" ]; then
    echo "Autoscaling Watchdog Starting Kinesis Autoscaler with config $1"
    cmd="java -cp target/KinesisScalingUtils-complete.jar -Dconfig-file-url=$1 com.amazonaws.services.kinesis.scaling.auto.AutoscalingController"

    `$cmd`
  else
    # exit if the watchdog is what is running
    watchdog_running=`ps -eaf | grep "watchdog.sh" | grep -v "grep" | wc -l | awk '{print $1}'`
    
    if [ "$watchdog_running" -ne "0" ]; then
      exit 0
    fi
  fi

  sleep 10
done
