#!/bin/bash

kill -9 `ps -ef | grep /home/centos/web-service/ems_anomaly_detector/main.py | awk '{print $2}'`