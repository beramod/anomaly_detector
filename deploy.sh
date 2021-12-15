#!/bin/bash

PROJECT_DIR=/home/centos/web-service/ems_anomaly_detector
cd ${PROJECT_DIR}
git pull
${PROJECT_DIR}/restart.sh
