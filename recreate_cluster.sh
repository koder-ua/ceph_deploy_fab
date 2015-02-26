#!/bin/bash
IMGS_DIR=/media/vms/tiny_cloud
DATA_SZ=5G
JOURNAL_SZ=1G
PASSWD=koder
USER=koder
BASE_IMG=ubuntu_base.qcow2

# MON_COUNT=1
OSD_COUNT=2
LAST_IDX=$(expr $OSD_COUNT - 1)
OSD_IDXS=$(seq 0 $LAST_IDX)

function stop_cluster() {
	tcloud stop ceph-mon

	# parralelize this loop
	for idx in $OSD_IDXS ; do
		tcloud stop "ceph-osd-$idx"
	done
}

function wait_cluster_stopped() {
	OSD_NAMES="osd-0"

	for idx in $(seq 1 $LAST_IDX) ; do
		OSD_NAMES="$OSD_NAMES\|osd-$idx"
	done

	vms="execute first step"
	while [ ! -z "$vms" ] ; do
		vms=$(tcloud list | grep "ceph-\(mon\|$OSD_NAMES)")
	done
}

function start_cluster() {
	tcloud start ceph-mon

	# parralelize this loop
	for idx in $OSD_IDXS ; do
		tcloud start ceph-osd-$idx
	done
}


function clear_images() {
	pushd $IMGS_DIR
	sudo rm -f ceph-mon.qcow2

	# parralelize this loop
	for idx in $OSD_IDXS ; do
		sudo rm -f ceph-osd-${idx}.qcow2
		sudo rm -f ceph-osd-${idx}-data.qcow2
		sudo rm -f ceph-osd-${idx}-journal.qcow2
	done

	popd
}


function create_images() {
	pushd $IMGS_DIR

	qemu-img create -f qcow2 -o backing_file=ubuntu_base.qcow2,backing_fmt=qcow2 ceph-mon.qcow2

	# parralelize this loop
	for idx in $OSD_IDXS ; do
		qemu-img create -f qcow2 -o backing_file=${BASE_IMG},backing_fmt=qcow2 ceph-osd-${idx}.qcow2
		qemu-img create -f qcow2 -o size=$DATA_SZ ceph-osd-${idx}-data.qcow2
		qemu-img create -f qcow2 -o size=$JOURNAL_SZ ceph-osd-${idx}-journal.qcow2
	done

	popd
}


function get_ip() {
	ip=
	while [ -z "$ip" ] ; do
		set +e
		ip=$(sudo tcloud list | grep $1 | awk '{print $4}')
		set -e
	done
	echo $ip
}


function prepare_cluster() {
	ssh_opts="-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"

	key_file=$(tempfile)
	rm "$key_file"
	ssh-keygen -t rsa -N '' -f "$key_file"

	NAMES="ceph-mon"
	for idx in $(seq 0 $LAST_IDX) ; do
		NAMES="$NAMES ceph-osd-${idx}"
	done

	# parralelize this loop
	for name in $NAMES ; do
		ip=$(get_ip $name)
		sshpass "-p${PASSWD}" ssh-copy-id $ssh_opts "${USER}@${ip}"
		ssh-copy-id $ssh_opts -i "$key_file" "${USER}@${ip}"
		scp $ssh_opts "$key_file" "${USER}@${ip}:/home/${USER}/.ssh/id_rsa"
		scp $ssh_opts "${key_file}.pub" "${USER}@${ip}:/home/${USER}/.ssh/id_rsa.pub"

		curr_hostname=$(ssh $ssh_opts "${USER}@${ip}" hostname)
		# update hostname
		ssh $ssh_opts "${USER}@${ip}" sudo hostname "$name"
		ssh $ssh_opts "${USER}@${ip}" echo "$name" "|" sudo tee /etc/hostname
		ssh $ssh_opts "${USER}@${ip}" sudo sed -i "/127.0.1.1/d" /etc/hosts
		ssh $ssh_opts "${USER}@${ip}" echo "127.0.1.1 $name" "|" sudo tee -a /etc/hosts
	done

	rm "$key_file"
	rm "${key_file}.pub"
}


set -x
stop_cluster
wait_cluster_stopped

set -e
clear_images
create_images
start_cluster
prepare_cluster

 