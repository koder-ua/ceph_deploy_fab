#!/bin/bash
set -x

# tcloud start ceph-mon ceph-osd-0 ceph-osd-1 ceph-osd-2

set -e
set -o pipefail

MON_IP=
while [ -z "$MON_IP" ] ; do
	IPS=$(sudo tcloud list)
	MON_IP=$(echo "$IPS" | grep 'ceph-mon' | awk '{print $4}')
done

ssh $MON_IP sudo start ceph-mon id=ceph-mon

function join(){ 
	local IFS="$1"
	shift
	echo "$*"
}

OSD_IPS_LIST=$(echo "$IPS" | grep 'ceph-osd' | awk '{print $4}')
OSD_IPS=$(join ',' $OSD_IPS_LIST)
fab --fabfile deploy_ceph.py -H "$OSD_IPS" start_osd_after_reboot

