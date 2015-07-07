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


stor_str_templ = """StorageNode({0.ip}):
    name: {0.name}
    rsync_ip: {0.rsync_ip}
    dev2dir:
{1}
"""


class Storage(Node):
    def __init__(self, name, ip, rsync_ip, mount_root, dev2dir):
        Node.__init__(self, name, ip)
        self.name = name
        self.ip = ip
        self.rsync_ip = rsync_ip
        self.dev2dir = dev2dir

    def __str__(self):
        dev2dirstrs = ["        {0}=>{1}".format(*itm)
                       for itm in self.dev2dir.items()]
        return stor_str_templ.format(self, "\n".join(dev2dirstrs))


class Nodes(object):
    def __init__(self):
        self.storage = []
        self.proxy = []
        self.controler = None
        self.all_ip = set()


@task
@parallel
def get_files(globs):
    return [i.strip() for i in run("ls -1 " + " ".join(globs)).split()]


def load_cfg(path):
    cfg = yaml.load(open(path).read())
    nodes = Nodes()
    idx = 0

    for name, node_config in cfg['storage_nodes'].items():
        ip = node_config['ip'].strip()
        rsync_ip = node_config['rsync_ip'].strip()

        dev2dir = {}
        mount_root = node_config['root_dir']

        if 'devs' in node_config:
            for pos, dev in enumerate(node_config['devs']):
                mpoint = os.path.join(mount_root, "dev" + str(pos))
                dev2dir[dev] = mpoint
        elif 'by_id' in node_config:
            for pos, dev_id in enumerate(node_config['by_id']):
                dev_id = str(dev_id).strip()
                mpoint = os.path.join(mount_root, "dev-" + dev_id)
                dev2dir["/dev/disk/by-id/" + dev_id] = mpoint
        elif 'globs' in node_config:
            paths = [item.strip() for item in node_config['globs']]
            files = execute(get_files, paths, hosts=[ip])[ip]
            for idx, dev in enumerate(files, idx):
                mdir = "dev-{0}-{1}".format(idx, os.path.basename(dev))
                dev2dir[dev] = os.path.join(mount_root, mdir)
        else:
            raise ValueError("Can't found any device config")

        st = Storage(name=name,
                     ip=ip,
                     rsync_ip=rsync_ip,
                     mount_root=node_config['root_dir'],
                     dev2dir=dev2dir)

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
def umount_all_swift(nodes, cfg):
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
def start_proxy():
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


def setup_rings(nodes, cfg):
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
                     ('object.builder', 6003)]

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
    obj_c = obj_c.replace("bind_port = 6000", "bind_port = 6003")
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


def update_fstab(mount_root, mpoints):
    fstab_sio = StringIO()
    get(remote_path='/etc/fstab', local_path=fstab_sio)
    fstab = fstab_sio.getvalue()

    new_fstab = []
    for line in fstab.split("\n"):
        pline = line.strip()
        if pline != "" and not pline.startswith("#"):
            mpoint = pline.split()[1]
            if not mpoint.startswith(mount_root):
                new_fstab.append(line)
        else:
            new_fstab.append(line)

    lt = "{0} {1} xfs noatime,nodiratime,nobarrier,logbufs=8 0 0"
    for dev, mpoint in mpoints:
        new_fstab.append(lt.format(dev, mpoint))

    put(remote_path='/etc/fstab',
        local_path=StringIO("\n".join(new_fstab) + "\n"),
        use_sudo=True)


@task
@parallel
def deploy_storage(nodes, cfg):
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

    xfs_opts = cfg.get("xfs_opts", "")
    for dev, mount_path in node.dev2dir.items():
        sudo("mkfs.xfs -f {0} {1}".format(xfs_opts, dev))
        sudo("mkdir -p " + mount_path)

    update_fstab(mount_root, node.dev2dir.items())

    for dev, mount_path in node.dev2dir.items():
        sudo("mount " + mount_path)

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
    sudo("rm -rf /var/cache/swift/*")
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
def start_storage():
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
        local_path=StringIO(cfg),
        use_sudo=True)


swift_rc_templ = """
export ST_AUTH="http://{0}:8080/auth/v1.0/"
export ST_USER="admin:admin"
export ST_KEY="admin"
export SW_NODES="{1}"
export SW_TOKEN=`curl -v -H  "X-Auth-User:$ST_USER" -H  "X-Auth-Key:$ST_KEY" "$ST_AUTH" 2>&1 | grep X-Auth-Token | awk '{{print $3}}'`
"""


@task
@parallel
def deploy_testnode(all_proxy):
    # if 'rh' == get_distro():
    #     sudo("systemctl stop firewalld.service", warn_only=True)
    #     sudo("systemctl disable firewalld.service", warn_only=True)
    #     with hide('stdout', 'stderr'):
    #         sudo("yum -y install epel-release")
    #         sudo("yum -y install http://rdo.fedorapeople.org/openstack-kilo/rdo-release-kilo.rpm")
    #         sudo("yum -y upgrade")
    #         sudo("yum -y install ntp")

    # sudo("groupadd swift")
    # sudo("useradd swift -g swift -M -n")

    # sudo("service ntpd stop", warn_only=True)
    # sudo("ntpdate pool.ntp.org", warn_only=True)
    # sudo("service ntpd start", warn_only=True)

    # sudo("rm /etc/localtime")
    # sudo("cp /usr/share/zoneinfo/Europe/Kiev /etc/localtime")
    # sudo("yum -y install python-swiftclient git ")
    # run("git clone https://github.com/markseger/getput.git")

    swift_rc = swift_rc_templ.format(all_proxy[0],
                                     ",".join(ip for ip in all_proxy))
    put(remote_path='swiftrc',
        local_path=StringIO(swift_rc))


if __name__ == "__main__":
    env.user = 'root'

    cmd, conf_path = sys.argv[1:]
    nodes, cfg = load_cfg(conf_path)

    all_stors = [storage.ip for storage in nodes.storage]
    all_proxy = [proxy.ip for proxy in nodes.proxy]
    all_mcache = [cfg['memcache_node'].strip()]
    testnodes = [ip.strip() for ip in cfg['testnodes']]

    all_swift = set(all_proxy)
    all_swift.update(all_stors)

    if cmd == 'clear':
        pass
    else:
        # execute(prepare, hosts=nodes.all_ip)

        # execute(stop_storage, hosts=all_stors)
        # execute(stop_proxy, hosts=all_proxy)
        # execute(stop_memcache, hosts=all_mcache)

        # execute(umount_all_swift, nodes, cfg, hosts=all_stors)

        # execute(deploy_memcache, hosts=all_mcache)
        # execute(deploy_proxy, all_mcache[0], hosts=all_proxy)
        # execute(deploy_storage, nodes, cfg, hosts=all_stors)

        # execute(setup_configs, hosts=all_stors)

        # swift_cfg = get_swift_cfg(all_stors, all_proxy, all_mcache)
        # execute(save_swift_cfg, swift_cfg, hosts=all_swift)

        # execute(setup_rings, nodes, cfg, hosts=[nodes.controler.ip])

        # execute(start_memcache, hosts=all_mcache)
        # execute(start_proxy, hosts=all_proxy)
        # execute(start_storage, hosts=all_stors)

        execute(deploy_testnode, all_proxy, hosts=testnodes)

    disconnect_all()
