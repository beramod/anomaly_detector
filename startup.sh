#!/bin/bash
source ~/.bashrc
PROJECT_DIR=/home/centos/web-service/ems_anomaly_detector
source ${PROJECT_DIR}/ems_anomaly_detector.profile
pyenv shell ems_anomaly_detector
./install.sh
nohup python ${PROJECT_DIR}/main.py > ${PROJECT_DIR}/var/log/ems_anomaly_detector.log & echo $! > ${PROJECT_DIR}/var/pid/ems_anomaly_detector.pid
${PROJECT_DIR}/pscheck.sh