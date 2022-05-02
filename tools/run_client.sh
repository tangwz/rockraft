#!/bin/bash

IP=`hostname -i | awk '{print $NF}'`

../build/rockraft_client -conf="${IP}:8100:0,${IP}:8101:0,${IP}:8102:0" -op="get" -key="test3" -value="ttttt" -log_each_request=true
