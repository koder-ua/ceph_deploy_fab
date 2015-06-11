import sys
import uuid
import hashlib
import urllib2
import os.path
from StringIO import StringIO

import yaml

from fabric.api import run, sudo, task
from fabric.network import disconnect_all
from fabric.context_managers import cd, hide
from fabric.contrib.files import append, exists
from fabric.api import parallel, put, env, execute, get


def get_distro():
    if exists("/etc/redhat-release"):
        return 'rh'
    return 'ubuntu'


class Node(object):
    def __init__(self, name, ip):
        self.name = name
        self.ip = ip


class Storage(Node):
    def __init__(self, name, ip, rsync_ip, mount_root, devs, scsi_ids):
        Node.__init__(self, name, ip)
        self.name = name
        self.ip = ip
        self.rsync_ip = rsync_ip

        assert devs is not None or scsi_ids is not None
        assert devs is None or scsi_ids is None

        if devs is None:
            self.dev2dir = None
        else:
            self.dev2dir = {}
            for pos, dev in enumerate(devs):
                mpoint = os.path.join(mount_root, "dev" + str(pos))
                self.dev2dir[dev] = mpoint

        if scsi_ids is None:
            self.scsi2dir = None
        else:
            self.scsi2dir = {}
            for pos, scsi_id in enumerate(scsi_ids):
                str_id = scsi_id.replace("[", "").replace("]", "").replace(":", "_")
                mpoint = os.path.join(mount_root, "dev" + str_id)
                self.scsi2dir[scsi_id] = mpoint


class Nodes(object):
    def __init__(self):
        self.storage = []
        self.proxy = []
        self.controler = None
        self.all_ip = set()


def load_cfg(path):
    cfg = yaml.load(open(path).read())
    nodes = Nodes()

    for name, node_config in cfg['storage_nodes'].items():
        ip = node_config['ip'].strip()
        rsync_ip = node_config['rsync_ip'].strip()

        devs = None
        scsi_ids = None
        if 'devs' in node_config:
            devs = ['/dev/' + dev for dev in node_config['devs']]
        else:
            assert 'scsi_luns' in node_config
            scsi_ids = [scsi_id.strip() for scsi_id in node_config['scsi_luns']]

        st = Storage(name=name,
                     ip=ip,
                     rsync_ip=rsync_ip,
                     mount_root=node_config['root_dir'],
                     devs=devs,
                     scsi_ids=scsi_ids)

        nodes.storage.append(st)
        nodes.all_ip.add(ip)

    for name, ip in cfg['proxy_nodes'].items():
        nodes.proxy.append(Node(name, ip.strip()))
        nodes.all_ip.add(ip.strip())
    nodes.controler = nodes.proxy[0]

    return nodes, cfg


#  --------------------------------------- BASIC PREPARE ----------------------------------------------

@task
@parallel
def prepare():
    if 'rh' == get_distro():
        sudo("systemctl stop firewalld.service", warn_only=True)
        sudo("systemctl disable firewalld.service", warn_only=True)
        with hide('stdout', 'stderr'):
            sudo("yum -y install epel-release")
            sudo("yum -y install http://rdo.fedorapeople.org/openstack-kilo/rdo-release-kilo.rpm")
            sudo("yum -y upgrade")
            sudo("yum -y install ntp")

    sudo("groupadd swift")
    sudo("useradd swift -g swift -M -n")

    sudo("rm /etc/localtime")
    sudo("cp /usr/share/zoneinfo/Europe/Kiev /etc/localtime")
    sudo("service ntpd stop", warn_only=True)
    sudo("ntpdate pool.ntp.org", warn_only=True)
    sudo("service ntpd start", warn_only=True)


prox_cfg = """bind_ip = 0.0.0.0
user = swift
swift_dir = /etc/swift"""


@task
@parallel
def umount_all_swift(config_path):
    nodes, cfg = load_cfg(config_path)
    hostname = run("hostname -s")
    mount_root = cfg['storage_nodes'][hostname]['root_dir'].strip()

    for line in run("mount").split("\n"):
        dev, _, path = line.split()[:3]
        if path.startswith(mount_root):
            sudo('umount ' + dev)


def get_ips(runner=run):
    return runner("/sbin/ip -4 -o addr show scope global |" +
                  " awk '{gsub(/\/.*/,\"\",$4); print $4}'")


# ------------------------- MEMCACHE --------------------------------------------------

@task
@parallel
def deploy_memcache():
    sudo("yum -y install memcached")


@task
@parallel
def start_memcache():
    sudo("systemctl enable memcached.service")
    sudo("systemctl start memcached.service")


@task
@parallel
def stop_memcache():
    sudo("systemctl stop memcached.service")
    sudo("systemctl disable memcached.service")


# ------------------------- PROXY --------------------------------------------------

@task
@parallel
def deploy_proxy(memcache_ip):
    sudo("yum -y install openstack-swift-proxy python-swiftclient")

    url = "https://git.openstack.org/cgit/openstack/swift/plain/etc/proxy-server.conf-sample?h=stable/kilo"
    prox = urllib2.urlopen(url).read()

    prox = prox.replace("# bind_ip = 0.0.0.0", prox_cfg)
    prox = prox.replace("# account_autocreate = false",
                        "account_autocreate = true")

    prox = prox.replace("# operator_roles = admin, swiftoperator",
                        "operator_roles = admin, swiftoperator")

    prox = prox.replace("# memcache_servers = 127.0.0.1:11211",
                        "memcache_servers = {0}:11211".format(memcache_ip))

    prox = prox.replace("# log_level = INFO", "log_level = ERROR")

    put(remote_path='/etc/swift/proxy-server.conf',
        local_path=StringIO(prox),
        use_sudo=True)


@task
@parallel
def start_proxy(swift_cfg):
    sudo("systemctl enable openstack-swift-proxy.service")
    sudo("systemctl start openstack-swift-proxy.service")


@task
@parallel
def stop_proxy():
    sudo("systemctl stop openstack-swift-proxy.service", warn_only=True)
    sudo("systemctl disable openstack-swift-proxy.service", warn_only=True)


# ------------------------- RINGS --------------------------------------------------


@task
@parallel
def store_rings(files):
    for fname, data in files.items():
        put(remote_path='/etc/swift/' + fname,
            local_path=StringIO(data.getvalue()),
            use_sudo=True)
    sudo("chown -R swift:swift /etc/swift")


def setup_rings(config_path):
    nodes, cfg = load_cfg(config_path)

    def forall_devs(cmd_templ):
        for node in nodes.storage:
            for mount in node.dev2dir.values():
                dev_fname = os.path.basename(mount.strip())
                sudo(cmd_templ.format(ip=node.ip, dev=dev_fname), user='swift')

    sudo("chown -R swift:swift /etc/swift")

    with cd("/etc/swift"):
        files = ['account.ring.gz', 'container.ring.gz', 'object.ring.gz',
                 'account.builder', 'container.builder', 'object.builder']
        run("rm -f " + " ".join(files))
        ring_port = [('account.builder', 6002),
                     ('container.builder', 6001),
                     ('object.builder', 6000)]

        for ring, port in ring_port:
            # Account ring
            sudo("swift-ring-builder {ring} create 10 3 1".format(ring=ring), user='swift')
            forall_devs("swift-ring-builder {ring} add r1z1-{{ip}}:{port}/{{dev}} 100".format(
                ring=ring, port=port))
            sudo("swift-ring-builder {ring} rebalance".format(ring=ring), user='swift')
            sudo("swift-ring-builder {ring}".format(ring=ring), user='swift')

        all_ips = list(set(nodes.all_ip) - set(get_ips().split()))

        if len(all_ips) > 0:
            dt = {}
            for fname in files:
                dt[fname] = StringIO()
                get(remote_path=fname, local_path=dt[fname])

            execute(store_rings, dt, hosts=all_ips)


rsync_conf_templ = """
uid = swift
gid = swift
log file = /var/log/rsyncd.log
pid file = /var/run/rsyncd.pid
address = {rsync_ip}

[account]
max connections = 2
path = /srv/node/
read only = false
lock file = /var/lock/account.lock

[container]
max connections = 2
path = /srv/node/
read only = false
lock file = /var/lock/container.lock

[object]
max connections = 2
path = /srv/node/
read only = false
lock file = /var/lock/object.lock
"""

acc_cfg = """bind_ip = 0.0.0.0
user = swift
swift_dir = /etc/swift
devices = /srv/node"""


cont_cfg = """bind_ip = 0.0.0.0
user = swift
swift_dir = /etc/swift
devices = /srv/node"""


obj_cfg = """bind_ip = 0.0.0.0
user = swift
swift_dir = /etc/swift
devices = /srv/node"""


def setup_configs():
    url = "https://git.openstack.org/cgit/openstack/swift/plain/etc/account-server.conf-sample?h=stable/kilo"
    acc = urllib2.urlopen(url).read()
    acc = acc.replace("# bind_ip = 0.0.0.0", acc_cfg)
    acc = acc.replace("# recon_cache_path = /var/cache/swift", "recon_cache_path = /var/cache/swift")
    acc = acc.replace("# log_level = INFO", "log_level = ERROR")
    put(remote_path='/etc/swift/account-server.conf',
        local_path=StringIO(acc),
        use_sudo=True)

    url = "https://git.openstack.org/cgit/openstack/swift/plain/etc/container-server.conf-sample?h=stable/kilo"
    cont = urllib2.urlopen(url).read()
    cont = cont.replace("# bind_ip = 0.0.0.0", cont_cfg)
    cont = cont.replace("# recon_cache_path = /var/cache/swift", "recon_cache_path = /var/cache/swift")
    cont = cont.replace("# log_level = INFO", "log_level = ERROR")
    put(remote_path='/etc/swift/container-server.conf',
        local_path=StringIO(cont),
        use_sudo=True)

    url = "https://git.openstack.org/cgit/openstack/swift/plain/etc/object-server.conf-sample?h=stable/kilo"
    obj_c = urllib2.urlopen(url).read()
    obj_c = obj_c.replace("# bind_ip = 0.0.0.0", obj_cfg)
    obj_c = obj_c.replace("# log_level = INFO", "log_level = ERROR")
    put(remote_path='/etc/swift/object-server.conf',
        local_path=StringIO(obj_c),
        use_sudo=True)

    sudo("curl -o /etc/swift/container-reconciler.conf " +
         "https://git.openstack.org/cgit/openstack/swift/plain/etc/container-reconciler.conf-sample?h=stable/kilo")

    sudo("curl -o /etc/swift/object-expirer.conf " +
         "https://git.openstack.org/cgit/openstack/swift/plain/etc/object-expirer.conf-sample?h=stable/kilo")


@task
@parallel
def setup_configs_task():
    setup_configs()


# ---------------------------------  STORAGE --------------------------------------------------------------

@task
@parallel
def get_scsi_dev_mapping():
    id2dev = {}
    with hide('stdout', 'stderr'):
        for line in run("lsscsi").split("\n"):
            vals = line.split()
            if len(vals) == 6:
                id2dev[vals[0]] = vals[5]
    return id2dev


def update_fstab():
    pass
    # fstab_sio = StringIO()
    # get(remote_path='/etc/fstab', local_path=fstab_sio)
    # fstab = fstab_sio.getvalue()
    # new_fstab = []
    # for line in fstab.split("\n"):
    #     pline = line.strip()
    #     if pline != "" and not pline.startswith("#"):
    #         mpoint = pline.split()[1]
    #         if not mpoint.startswith(mount_root):
    #             new_fstab.append(line)
    #     else:
    #         new_fstab.append(line)

    # dev_uuid = run("blkid " + dev).split()[1].split('"')[2]
    # line = "UUID={0} {1} xfs noatime,nodiratime,nobarrier,logbufs=8 0 0".format(dev_uuid, mount_path)
    # new_fstab.append(line)

    # put(remote_path='/etc/fstab',
    #     local_path=StringIO("\n".join(new_fstab)),
    #     use_sudo=True)

    # for dev, mount_path in node.dev2dir.items():
    #     sudo("mount " + mount_path)
    #     sudo("chown -R swift:swift " + mount_path)


@task
@parallel
def deploy_storage(config_path):
    nodes, cfg = load_cfg(config_path)
    hostname = run("hostname -s")
    for node in nodes.storage:
        if node.name == hostname:
            break
    else:
        raise ValueError("No node {0} found in config storage nodes".format(hostname))

    sudo("yum -y install xfsprogs rsync openstack-swift-account" +
         " openstack-swift-container openstack-swift-object")

    mount_root = cfg['storage_nodes'][hostname]['root_dir'].strip()
    assert mount_root != ""
    sudo("rmdir {0}/*".format(mount_root), warn_only=True)

    dev2mp = []

    if node.dev2dir is not None:
        dev2mp = node.dev2dir.items()
    else:
        id2dev = get_scsi_dev_mapping()
        for scsi_id, mount_path in node.scsi2dir.items():
            dev2mp.append((id2dev[scsi_id], mount_path))

    for dev, mount_path in dev2mp:
        sudo("mkfs.xfs -f " + dev)
        sudo("mkdir -p " + mount_path)
        sudo("mount -o noatime,nodiratime,nobarrier,logbufs=8 {0} {1}".format(dev, mount_path))

        assert mount_path != "" and mount_path != "/"
        sudo("rm -rf {0}/*".format(mount_path), warn_only=True)

        sudo("chown -R swift:swift " + mount_path)

    rsync_conf = rsync_conf_templ.format(rsync_ip=node.rsync_ip)
    put(remote_path='/etc/rsyncd.conf',
        local_path=StringIO(rsync_conf),
        use_sudo=True)

    sudo("systemctl enable rsyncd.service")
    sudo("systemctl start rsyncd.service")

    setup_configs()
    sudo("mkdir -p /var/cache/swift")
    sudo("chown -R swift:swift /etc/swift /var/cache/swift")


storage_services = """
    openstack-swift-account.service
    openstack-swift-account-auditor.service
    openstack-swift-account-reaper.service
    openstack-swift-account-replicator.service
    openstack-swift-container.service
    openstack-swift-container-auditor.service
    openstack-swift-container-replicator.service
    openstack-swift-container-updater.service
    openstack-swift-object.service
    openstack-swift-object-auditor.service
    openstack-swift-object-replicator.service
    openstack-swift-object-updater.service"""

storage_services = " ".join(storage_services.split())


@task
@parallel
def start_storage(swift_cfg):
    put(remote_path='/etc/swift/swift.conf',
        local_path=StringIO(swift_cfg),
        use_sudo=True)
    sudo("chown -R swift:swift /etc/swift /var/cache/swift")

    sudo("systemctl enable " + storage_services)
    sudo("systemctl start " + storage_services)


@task
@parallel
def stop_storage():
    sudo("systemctl stop " + storage_services, warn_only=True)
    sudo("systemctl disable " + storage_services, warn_only=True)


def get_swift_cfg(all_stors, all_proxy, all_mcache):
    url = "https://git.openstack.org/cgit/openstack/swift/plain/etc/swift.conf-sample?h=stable/kilo"
    swift_cfg = urllib2.urlopen(url).read()

    suff = hashlib.md5(str(uuid.uuid1())).hexdigest()
    swift_cfg = swift_cfg.replace("swift_hash_path_suffix = changeme",
                                  "swift_hash_path_suffix = " + suff)

    suff = hashlib.md5(str(uuid.uuid1())).hexdigest()
    return swift_cfg.replace("swift_hash_path_prefix = changeme",
                             "swift_hash_path_prefix = " + suff)


def save_swift_cfg(cfg):
    put(remote_path='/etc/swift/swift.conf',
        local_path=StringIO(swift_cfg),
        use_sudo=True)


if __name__ == "__main__":
    cmd, conf_path = sys.argv[1:]
    nodes, cfg = load_cfg(conf_path)

    all_stors = [storage.ip for storage in nodes.storage]
    all_proxy = [proxy.ip for proxy in nodes.proxy]
    all_mcache = [cfg['memcache_node'].strip()]

    all_swift = set(all_proxy)
    all_swift.update(all_stors)

    env.user = 'root'

    if cmd == 'clear':
        pass
    else:
        # execute(prepare, hosts=nodes.all_ip)

        execute(stop_storage, hosts=all_stors)
        execute(stop_proxy, hosts=all_proxy)
        execute(stop_memcache, hosts=all_mcache)

        execute(umount_all_swift, conf_path, hosts=all_stors)

        execute(deploy_memcache, hosts=all_mcache)
        execute(deploy_proxy, all_mcache[0], hosts=all_proxy)
        execute(deploy_storage, conf_path, hosts=all_stors)

        execute(setup_configs, hosts=all_stors)

        swift_cfg = get_swift_cfg(all_stors, all_proxy, all_mcache)
        execute(save_swift_cfg, swift_cfg, hosts=all_swift)

        execute(setup_rings, conf_path, hosts=[nodes.controler.ip])

        execute(start_memcache, hosts=all_mcache)
        execute(start_proxy, swift_cfg, hosts=all_proxy)
        execute(start_storage, swift_cfg, hosts=all_stors)

    disconnect_all()
