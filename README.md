## Fabric scripts to deploy ceph

How to use: edit ceph_conf.yaml accordingly to you lab

* changing cluster name from 'ceph' don't works now *

deploy monitor

	$ fab --fabfile deploy_ceph.py -H MON_IP_1 deploy_first_mon

deploy osd's

	$ fab --fabfile deploy_ceph.py -H OSD_IP_1,OSD_IP_2,... add_new_osd

You should have password-less access to all nodes.
Password less sudo should be setupped for login user


