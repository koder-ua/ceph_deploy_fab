import re
import uuid
import os.path
from StringIO import StringIO

import yaml

from fabric.api import run, sudo, execute, task, get, put, local, env
from fabric.api import parallel
from fabric.context_managers import hide
from fabric.contrib.files import append, exists


ceph_config_file_templ_path = os.path.join(os.path.dirname(__file__),
                                           'ceph_conf.templ')
deployment_config = os.path.join(os.path.dirname(__file__),
                                 'deployment_conf.yaml')
ceph_config_file_templ = open(ceph_config_file_templ_path).read()


def prepare_node(release):
    if exists("/etc/redhat-release"):
        return
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
def deploy_first_mon(conf_path=deployment_config):
    params = get_config(conf_path)
    params.fsid_uuid = str(uuid.uuid4())
    params.mon_ip = env.host_string

    prepare_node(params.ceph_release)

    ceph_config_file = ceph_config_file_templ.format(params)

    if params.fs_type == 'ext4':
        ceph_config_file += '\nfilestore xattr use omap = true\n'

    sudo("rm -f {0.ceph_cfg_path}".format(params))

    put(remote_path=params.ceph_cfg_path,
        local_path=StringIO(ceph_config_file),
        use_sudo=True)

    commands_templ = """
        sudo ceph-authtool --create-keyring {0.mon_keyring_path}
            --gen-key -n mon. --cap mon 'allow *'

        sudo ceph-authtool --create-keyring {0.admin_keyring_path}
            --gen-key -n client.admin
            --set-uid=0 --cap mon 'allow *' --cap osd 'allow *'
             --cap mds 'allow'

        sudo ceph-authtool {0.mon_keyring_path} --import-keyring
            {0.admin_keyring_path}

        sudo monmaptool --create --add {0.hostname} {0.mon_ip}
            --fsid {0.fsid_uuid} {0.monmap_path}

        sudo mkdir /var/lib/ceph/mon/{0.clustername}-{0.hostname}

        sudo ceph-mon --mkfs -i {0.hostname} --monmap {0.monmap_path}
            --keyring {0.mon_keyring_path}

        sudo touch "/var/lib/ceph/mon/{0.clustername}-{0.hostname}/done"

        sudo chmod a+r {0.admin_keyring_path}

        sudo start ceph-mon id={0.hostname}

        ceph osd lspools

        ceph -s"""

    for cmd in prepare_cmds(commands_templ.format(params)):
        run(cmd)


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


def prepare_cmds(commands):
    result = [""]
    for cmd in commands.split("\n\n"):
        result.append(" ".join(
                i.strip() for i in cmd.split("\n") if i.strip() != "")
        )
    return result


def listdir_remote(path):
    return run('ls "{0}"'.format(path)).split()


@task
@parallel
def start_osd_after_reboot(conf_path=deployment_config):
    params = get_config(conf_path)
    mount_dir, mount_point = params.data_mount_path.rsplit('/', 1)

    assert '{' not in mount_dir
    assert '}' not in mount_dir
    assert mount_point == '{0.clustername}-{0.osd_num}'

    mpoints = listdir_remote(mount_dir)

    osd_nums = [int(name.split("-")[1])
                for name in mpoints
                if name.startswith(params.clustername)]

    assert len(osd_nums) == 1
    params.osd_num = osd_nums[0]
    params.data_mount_path = params.data_mount_path.format(params)

    commands_templ = """
    sudo mount {0.mount_opst} {0.osd_data_dev} {0.data_mount_path}

    sudo start ceph-osd id={0.osd_num}
    """

    for cmd in prepare_cmds(commands_templ.format(params)):
        run(cmd)


@task
@parallel
def add_new_osd(mon_ip, conf_path=deployment_config):
    params = get_config(conf_path)
    params.osd_uuid = str(uuid.uuid4())
    params.mon_ip = mon_ip

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

    commands_templ = """
    sudo mkdir -p {0.data_mount_path}

    sudo mkfs -t {0.fs_type} {0.osd_data_dev}

    sudo mount {0.mount_opst} {0.osd_data_dev} {0.data_mount_path}

    sudo ceph-osd -c {0.ceph_cfg_path} -i {0.osd_num}
        --cluster {0.clustername} --mkfs --mkkey --osd-uuid {0.osd_uuid}

    sudo ceph auth add osd.{0.osd_num} osd 'allow *' mon 'allow profile osd' -i
        /var/lib/ceph/osd/{0.clustername}-{0.osd_num}/keyring

    ceph osd crush add-bucket {0.hostname} host

    ceph osd crush move {0.hostname} root=default

    sudo start ceph-osd id={0.osd_num}

    ceph osd crush add {0.osd_num} {0.osd_weigth} host={0.hostname}

    ceph -s
    """

    for cmd in prepare_cmds(commands_templ.format(params)):
        run(cmd)


@task
@parallel
def netapp_add_new_osd(mon_ip, conf_path=deployment_config):
    params = get_config(conf_path)
    params.mon_ip = mon_ip

    # executed on mon host

    osd_devs = run("ls -1 /dev/sd*").split()
    # select HDD devices
    osd_devs = [dev for dev in osd_devs if len(os.path.nasename(dev)) == 4]

    # executed on osd host
    prepare_node(params.ceph_release)

    # remove journal section
    cfg_templ = re.sub(r"(?ims)^osd journal = .*$", "", ceph_config_file_templ)

    # put ceph config
    osd_config_file = cfg_templ.format(params)

    put(remote_path=params.ceph_cfg_path,
        local_path=StringIO(osd_config_file),
        use_sudo=True)

    # put admin keyring
    put(remote_path=params.admin_keyring_path,
        local_path=StringIO(params.admin_keyring_content),
        use_sudo=True)

    commands_templ = """
    sudo mkdir -p {0.data_mount_path}

    sudo mkfs -t {0.fs_type} {0.osd_data_dev}

    sudo mount {0.mount_opst} {0.osd_data_dev} {0.data_mount_path}

    sudo ceph-osd -c {0.ceph_cfg_path} -i {0.osd_num}
        --cluster {0.clustername} --mkfs --mkkey --osd-uuid {0.osd_uuid}

    sudo ceph auth add osd.{0.osd_num} osd 'allow *' mon 'allow profile osd' -i
        /var/lib/ceph/osd/{0.clustername}-{0.osd_num}/keyring

    ceph osd crush add-bucket {0.hostname} host

    ceph osd crush move {0.hostname} root=default

    sudo start ceph-osd id={0.osd_num}

    ceph osd crush add {0.osd_num} {0.osd_weigth} host={0.hostname}

    ceph -s
    """

    keys = {'fsid': 'fsid_uuid', 'mon initial members': 'mons'}

    print ">>>>>>>>>>>>>>>>>>>", osd_devs

    for dev in osd_devs:
        params.osd_uuid = str(uuid.uuid4())

        res = execute(allocate_osd_id_and_read_config, params, keys,
                      hosts=[params.mon_ip])

        for attr, val in res[params.mon_ip].items():
            setattr(params, attr, val)

        params.data_mount_path = params.data_mount_path.format(params)

        for cmd in prepare_cmds(commands_templ.format(params)):
            run(cmd)


@task
def gather_ceph_config(conf_path=deployment_config):
    params = get_config(conf_path)
    ceph_cfg_dir = os.path.dirname(params.ceph_cfg_path)

    if not os.path.exists(ceph_cfg_dir):
        local("sudo mkdir {0}".format(ceph_cfg_dir))

    local("sudo chmod a+w {0}".format(ceph_cfg_dir))

    if os.path.exists(params.ceph_cfg_path):
        local("sudo rm -f {0.ceph_cfg_path}".format(params))
    get(params.ceph_cfg_path, params.ceph_cfg_path)

    if os.path.exists(params.admin_keyring_path):
        local("sudo rm -f {0.admin_keyring_path}".format(params))
    get(params.admin_keyring_path, params.admin_keyring_path)

    local("sudo chmod a-w {0}".format(ceph_cfg_dir))
    local("sudo chmod a+r {0}".format(params.admin_keyring_path))
    local("sudo chmod a+r {0}".format(params.ceph_cfg_path))


@task
@parallel
def add_new_radosgw(conf_path=deployment_config):
    sudo("apt-get install apache2 libapache2-mod-fastcgi openssl " +
         "ssl-cert radosgw radosgw-agent")

    fqdn = run("hostname -f")
    append('/etc/apache2/apache2.conf', "ServerName " + fqdn)
    sudo("a2enmod rewrite")
    sudo("a2enmod fastcgi")
    sudo("a2enmod ssl")
    sudo("mkdir /etc/apache2/ssl")
    sudo("openssl req -x509 -nodes -days 365 -newkey rsa:2048 " +
         "-keyout /etc/apache2/ssl/apache.key -out " +
         "/etc/apache2/ssl/apache.crt")
    sudo("service apache2 restart")

    # config radosgw
