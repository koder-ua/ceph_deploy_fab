#!/bin/bash
set -x
set -e
set -o pipefail

function split_osd_jornals() {
    local node="$1"
    local dev="$2"
    local SSH="ssh $node"

    $SSH sudo parted -a optimal --script $dev -- mktable gpt
    $SSH sudo parted -a optimal --script $dev -- mkpart xfs 1 20GB
    $SSH sudo parted -a optimal --script $dev -- mkpart xfs 20GB 40GB
    $SSH sudo parted -a optimal --script $dev -- mkpart xfs 40GB 60GB
    $SSH sudo parted -a optimal --script $dev -- mkpart xfs 60GB 80GB
    $SSH sudo parted -a optimal --script $dev -- mkpart xfs 80GB 100GB
}

function pudge_ceph() {
    local NODES="$@"

    ceph-deploy purge $NODES
    ceph-deploy purgedata $NODES
}

function deploy_ceph() {
    local MONS="$1"
    local OSDS="$2"
    local CLIENTS="$3"

    IFS=' ' read -a OSD_DISKS <<< "$4"
    IFS=' ' read -a OSD_JOURNALS <<< "$5"

    local MON_0=$(echo $MONS | awk '{print $1}')
    local DISKS_LEN_PLUSONE=${#OSD_DISKS[@]}

    set +e
    local DISKS_LEN=$(expr $DISKS_LEN_PLUSONE - 1)
    set -e

    ceph-deploy --overwrite-conf new "$MON_0"
    ceph-deploy --overwrite-conf install $MONS $OSDS $CLIENTS
    ceph-deploy --overwrite-conf mon create-initial

    for osd in $OSDS; do
        for disk in ${OSD_DISKS[@]} ; do
            ceph-deploy disk zap "$osd:$disk"
        done
        # wait
    done

    local JOUNED_DISKS=""
    local JOUNED_PARTS=""
    for idx in $(seq 0 $DISKS_LEN) ; do
        JOUNED_DISKS="$JOUNED_DISKS ${OSD_DISKS[$idx]}:/dev/${OSD_JOURNALS[$idx]}"
        if [[ ${OSD_JOURNALS[$idx]} =~ ^.*[0-9]$ ]] ; then
            JOUNED_PARTS="$JOUNED_PARTS ${OSD_DISKS[$idx]}1:/dev/${OSD_JOURNALS[$idx]}"
        else
            JOUNED_PARTS="$JOUNED_PARTS ${OSD_DISKS[$idx]}1:/dev/${OSD_JOURNALS[$idx]}1"
        fi
    done

    for osd in $OSDS; do
        for DISK_PAIR in $JOUNED_DISKS ; do
            ceph-deploy osd prepare "$osd:$DISK_PAIR"
        done
        # wait
    done


    for osd in $OSDS; do
        for PARTITIONS_PAIR in $JOUNED_PARTS ; do
            ceph-deploy osd activate "$osd:/dev/$PARTITIONS_PAIR"
        done
    done

    ceph-deploy admin $MONS $OSDS $CLIENTS

    local FILES="ceph.bootstrap-mds.keyring ceph.bootstrap-osd.keyring ceph.conf "
    FILES="$FILES ceph.bootstrap-rgw.keyring ceph.client.admin.keyring ceph.mon.keyring"
    sudo cp $FILES /etc/ceph
}

# MONS="ceph-mon"
# OSDS="ceph-osd-0 ceph-osd-1 ceph-osd-2"
# OSD_DISKS="sdb"
# OSD_JOURNALS="sdc"

MONS="cz7625"
CLIENTS="cz7626 cz7627"
OSDS="cz7644 cz7645 cz7646 cz7647 cz7648"

pudge_ceph $MONS $OSDS $CLIENTS

# for node in $OSDS ; do
#     split_osd_jornals $node /dev/sdc
#     split_osd_jornals $node /dev/sdd
#     split_osd_jornals $node /dev/sde
#     split_osd_jornals $node /dev/sdf
# done

OSD_DISKS="sdg sdh sdi sdj sdk sdl sdm sdn sdo sdp sdq sdr sds sdt sdu sdv sdw sdx sdy sdz"
OSD_JOURNALS="sdc1 sdc2 sdc3 sdc4 sdc5 sdd1 sdd2 sdd3 sdd4 sdd5 sde1 sde2 sde3 sde4 sde5 sdf1 sdf2 sdf3 sdf4 sdf5"

deploy_ceph "$MONS" "$OSDS" "$CLIENTS" "$OSD_DISKS" "$OSD_JOURNALS"

