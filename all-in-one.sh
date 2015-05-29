#!/bin/bash
set -x
set -e

USER=koder
PASSWD=koder

# bash recreate_cluster.sh $USER $PASSWD

MON_IP=$(sudo tcloud list | grep ceph-mon | awk '{print $4}')
fab --fabfile deploy_ceph.py -H $MON_IP deploy_first_mon

OSD_IPS=$(sudo tcloud list | grep ceph-osd | awk '{print $4}')
OSD_IPS=$(echo $OSD_IPS | sed 's\ \,\g')

fab --fabfile deploy_ceph.py -H $OSD_IPS add_new_osd:$MON_IP
fab --fabfile deploy_ceph.py -H $MON_IP gather_ceph_config

ceph -s
