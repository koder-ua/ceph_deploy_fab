import re
import uuid
from StringIO import StringIO

import yaml

from fabric.api import run, sudo, execute, task, get, put
from fabric.api import parallel
from fabric.context_managers import hide


ceph_config_file_templ = """
[global]
fsid = {0.fsid_uuid}
mon initial members = {0.hostname}
mon host = {0.mon_ip}
public network = {0.pub_network}
cluster network = {0.cluster_network}
auth cluster required = cephx
auth service required = cephx
auth client required = cephx

osd journal size = {0.journal_sz}
osd pool default size = {0.default_pool_sz}
osd pool default min size = {0.default_min_sz}
osd pool default pg num = {0.default_pg_num}
osd pool default pgp num = {0.default_pgp_num}
osd crush chooseleaf type = {0.crush_chooseleaf_type}
"""


def prepare_node(release):
    add_ceph_dev_repo_keys = "wget -q -O- " + \
                             "'https://ceph.com/git/?p=ceph.git" + \
                             ";a=blob_plain;f=keys/release.asc' " + \
                             "| apt-key add -"
    sudo(add_ceph_dev_repo_keys)

    add_ceph_dev_repo_templ = "echo deb http://ceph.com/" + \
                              "debian-{0}/ " + \
                              "$(lsb_release -sc) main | tee /etc/apt/" + \
                              "sources.list.d/ceph.list"
    sudo(add_ceph_dev_repo_templ.format(release))

    with hide('stdout', 'stderr'):
        sudo("apt-get update")
        sudo("apt-get install -y ntp ceph ceph-mds")


def get_config(conf_path):

    cfg = yaml.load(open(conf_path).read())

    class Params(object):
        locals().update(cfg)

    Params.hostname = run("hostname")
    Params.monmap_path = Params.monmap_path.format(Params)
    Params.ceph_cfg_path = Params.ceph_cfg_path.format(Params)
    Params.mon_keyring_path = Params.mon_keyring_path.format(Params)
    Params.admin_keyring_path = Params.admin_keyring_path.format(Params)

    return Params


@task
def deploy_first_mon(conf_path="ceph_conf.yaml"):
    params = get_config(conf_path)
    params.fsid_uuid = str(uuid.uuid4())

    prepare_node(params.ceph_release)

    ceph_config_file = ceph_config_file_templ.format(params)

    if params.fs_type == 'ext4':
        ceph_config_file += '\nfilestore xattr use omap = true\n'

    sudo("rm -f {0.ceph_cfg_path}".format(params))
    put(remote_path=params.ceph_cfg_path,
        local_path=StringIO(ceph_config_file),
        use_sudo=True)

    templ = "ceph-authtool --create-keyring {0.mon_keyring_path}" + \
            " --gen-key -n mon. --cap mon 'allow *'"
    sudo(templ.format(params))

    templ = "ceph-authtool --create-keyring {0.admin_keyring_path} " + \
            "--gen-key -n client.admin " + \
            "--set-uid=0 --cap mon 'allow *' --cap osd 'allow *'" + \
            " --cap mds 'allow'"
    sudo(templ.format(params))

    templ = "ceph-authtool {0.mon_keyring_path} --import-keyring " + \
            "{0.admin_keyring_path}"
    sudo(templ.format(params))

    cmd = "monmaptool --create --add {0.hostname} {0.mon_ip}" + \
          " --fsid {0.fsid_uuid} {0.monmap_path}"
    sudo(cmd.format(params))

    templ = "mkdir /var/lib/ceph/mon/{0.clustername}-{0.hostname}"
    sudo(templ.format(params))

    mkfs_templ = "ceph-mon --mkfs -i {0.hostname} --monmap {0.monmap_path}" + \
                 " --keyring {0.mon_keyring_path}"
    sudo(mkfs_templ.format(params))

    touch_resdy = 'touch "/var/lib/ceph/mon/{0.clustername}-{0.hostname}/done"'
    sudo(touch_resdy.format(params))
    sudo("chmod a+r {0.admin_keyring_path}".format(params))

    sudo("start ceph-mon id={0.hostname}".format(params))

    run("ceph osd lspools")
    run("ceph -s")


@task
@parallel
def allocate_osd_id_and_read_config(params, keys):
    res = {'osd_num': run("ceph osd create {0.osd_uuid}".format(params))}
    # res = {'osd_num': '0'}

    fd = StringIO()
    get(params.ceph_cfg_path, fd)
    content = fd.getvalue()

    for line in content.split("\n"):
        if '=' in line:
            name, val = line.split("=", 1)
            name = re.sub(r"\s+", name.strip(), " ")
            val = val.strip()

            if name in keys:
                res[keys[name]] = val

    fd = StringIO()
    get(params.admin_keyring_path, fd)
    res['admin_keyring_content'] = fd.getvalue()

    return res


@task
@parallel
def add_new_osd(conf_path="ceph_conf.yaml"):
    params = get_config(conf_path)
    params.osd_uuid = str(uuid.uuid4())

    # executed on mon host

    keys = {'fsid': 'fsid_uuid', 'mon initial members': 'mons'}

    res = execute(allocate_osd_id_and_read_config, params, keys,
                  hosts=[params.mon_ip])

    for attr, val in res[params.mon_ip].items():
        setattr(params, attr, val)

    # executed on osd host
    prepare_node(params.ceph_release)

    # put ceph config
    osd_config_file = ceph_config_file_templ.format(params)

    put(remote_path=params.ceph_cfg_path,
        local_path=StringIO(osd_config_file),
        use_sudo=True)

    # put admin keyring
    put(remote_path=params.admin_keyring_path,
        local_path=StringIO(params.admin_keyring_content),
        use_sudo=True)

    params.data_mount_path = params.data_mount_path.format(params)

    sudo("mkdir -p {0.data_mount_path}".format(params))
    sudo("mkfs -t {0.fs_type} {0.osd_data_dev}".format(params))

    templ = "mount {0.mount_opst} {0.osd_data_dev} {0.data_mount_path}"
    sudo(templ.format(params))

    templ = "ceph-osd -c {0.ceph_cfg_path} -i {0.osd_num}" + \
            " --cluster {0.clustername} " + \
            "--mkfs --mkkey --osd-uuid {0.osd_uuid}"
    sudo(templ.format(params))

    templ = "ceph auth add osd.{0.osd_num} osd 'allow *' " + \
            " mon 'allow profile osd' -i " + \
            "/var/lib/ceph/osd/{0.clustername}-{0.osd_num}/keyring"
    sudo(templ.format(params))

    run("ceph osd crush add-bucket {0.hostname} host".format(params))
    run("ceph osd crush move {0.hostname} root=default".format(params))
    templ = "ceph osd crush add {0.osd_num} {0.osd_weigth} " + \
            "host={0.hostname}"
    sudo("start ceph-osd id={0.osd_num}".format(params))
    run(templ.format(params))
    run("ceph -s")
