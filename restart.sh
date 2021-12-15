#!/bin/bash

PROJECT_DIR=/home/centos/web-service/ems_anomaly_detector
${PROJECT_DIR}/shutdown.sh
sleep 2s
${PROJECT_DIR}/startup.sh