import sys
import uuid
import hashlib
import urllib2
from StringIO import StringIO

import yaml

from fabric.context_managers import cd
from fabric.api import run, sudo, task
from fabric.network import disconnect_all
from fabric.contrib.files import append, exists
from fabric.api import parallel, put, env, execute


def get_distro():
    if exists("/etc/redhat-release"):
        return 'rh'
    return 'ubuntu'


class Node(object):
    def __init__(self, name, ip, dev2dir=None):
        self.name = name
        self.ip = ip
        self.dev2dir = {} if dev2dir is None else dev2dir


class Nodes(object):
    def __init__(self):
        self.storage = []
        self.proxy = []
        self.controler = None
        self.all_ip = set()


@task
@parallel
def prepare():
    if 'rh' == get_distro():
        sudo("systemctl stop firewalld.service", warn_only=True)
        # with hide('stdout', 'stderr'):
        #     sudo("yum -y install epel-release")
        #     sudo("yum -y install http://rdo.fedorapeople.org/openstack-kilo/rdo-release-kilo.rpm")
        #     sudo("yum -y upgrade")
        #     sudo("yum -y install ntp openstack-selinux")

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


def load_cfg(path):
    cfg = yaml.load(open(path).read())
    nodes = Nodes()

    for name, node_config in cfg['storage_nodes'].items():
        ip = node_config['ip'].strip()
        nodes.storage.append(Node(name, ip, node_config['devs']))
        nodes.all_ip.add(ip)

    for name, ip in cfg['proxy_nodes'].items():
        nodes.proxy.append(Node(name, ip.strip()))
        nodes.all_ip.add(ip.strip())

    nodes.controler = nodes.proxy[0]

    return nodes, cfg


def get_ips(runner=run):
    return runner("/sbin/ip -4 -o addr show scope global |" +
                  " awk '{gsub(/\/.*/,\"\",$4); print $4}'")


@task
@parallel
def deploy_controller():
    # openstack user create --password-prompt swift
    # openstack role add --project service --user swift admin
    # openstack service create --name swift --description "OpenStack Object Storage" object-store

    # openstack endpoint create --publicurl 'http://controller:8080/v1/AUTH_%(tenant_id)s'
    #           --internalurl 'http://controller:8080/v1/AUTH_%(tenant_id)s'
    #           --adminurl http://controller:8080 --region RegionOne object-store
    # sudo("yum -y install python-keystone-auth-token python-keystonemiddleware")

    sudo("yum -y install openstack-swift-proxy python-swiftclient memcached")
    url = "https://git.openstack.org/cgit/openstack/swift/plain/etc/proxy-server.conf-sample?h=stable/kilo"
    prox = urllib2.urlopen(url).read()
    prox = prox.replace("# bind_ip = 0.0.0.0", prox_cfg)
    prox = prox.replace("# account_autocreate = false", "account_autocreate = true")
    prox = prox.replace("# operator_roles = admin, swiftoperator", "operator_roles = admin, swiftoperator")
    prox = prox.replace("# memcache_servers = 127.0.0.1:11211", "memcache_servers = 127.0.0.1:11211")
    put(remote_path='/etc/swift/proxy-server.conf',
        local_path=StringIO(prox),
        use_sudo=True)


def setup_rings(config_path):
    nodes, cfg = load_cfg(config_path)

    def forall_devs(cmd_templ):
        for node in nodes.storage:
            for dev, mount in node.dev2dir.items():
                dev_fname = dev.strip().split('/')[-1]
                sudo(cmd_templ.format(ip=node.ip, dev=dev_fname), user='swift')

    sudo("chown -R swift:swift /etc/swift")

    with cd("/etc/swift"):
        ring_port = [('account.builder', 6002),
                     ('container.builder', 6001),
                     ('object.builder', 6000)]

        for ring, port in ring_port:
            # Account ring
            sudo("swift-ring-builder {ring} create 10 3 1".format(ring=ring), user='swift')
            forall_devs("swift-ring-builder {ring} add r1z1-{{ip}}:{port}/{{dev}} 100".format(
                ring=ring, port=port))
            sudo("swift-ring-builder {ring}".format(ring=ring), user='swift')
            sudo("swift-ring-builder {ring} rebalance".format(ring=ring), user='swift')

        for ip in set(nodes.all_ip) - set(get_ips().split()):
            for fname in ['account.ring.gz', 'container.ring.gz', 'object.ring.gz']:
                sudo("scp {0} {1}:/etc/swift/{0}".format(fname, ip))
            sudo("ssh {0} chown -r swift:swift /etc/swift".format(ip))


rsync_conf_templ = """
uid = swift
gid = swift
log file = /var/log/rsyncd.log
pid file = /var/run/rsyncd.pid
address = {manage_ip}

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


obj_cfg = """bind_ip = 0.0.0.0
user = swift
swift_dir = /etc/swift
devices = /srv/node"""


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

    for dev, mount_path in node.dev2dir.items():
        sudo("mkfs.xfs " + dev)
        sudo("mkdir -p " + mount_path)
        line = "{0} {1} xfs noatime,nodiratime,nobarrier,logbufs=8 0 0\n".format(dev, mount_path)
        append("/etc/fstab", line, use_sudo=True)
        sudo("mount " + mount_path)
        sudo("chown -R swift:swift " + mount_path)

    rsync_conf = rsync_conf_templ.format(manage_ip=node.ip)
    put(remote_path='/etc/rsyncd.conf',
        local_path=StringIO(rsync_conf),
        use_sudo=True)

    sudo("systemctl enable rsyncd.service")
    sudo("systemctl start rsyncd.service")

    url = "https://git.openstack.org/cgit/openstack/swift/plain/etc/account-server.conf-sample?h=stable/kilo"
    acc = urllib2.urlopen(url).read()
    acc = acc.replace("# bind_ip = 0.0.0.0", acc_cfg)
    acc = acc.replace("# recon_cache_path = /var/cache/swift", "recon_cache_path = /var/cache/swift")
    put(remote_path='/etc/swift/account-server.conf',
        local_path=StringIO(acc),
        use_sudo=True)

    sudo("curl -o /etc/swift/container-server.conf " +
         "https://git.openstack.org/cgit/openstack/swift/plain/etc/container-server.conf-sample?h=stable/kilo")

    url = "https://git.openstack.org/cgit/openstack/swift/plain/etc/object-server.conf-sample?h=stable/kilo"
    obj_c = urllib2.urlopen(url).read()
    obj_c = obj_c.replace("# bind_ip = 0.0.0.0", obj_cfg)
    put(remote_path='/etc/swift/object-server.conf',
        local_path=StringIO(obj_c),
        use_sudo=True)

    sudo("curl -o /etc/swift/container-reconciler.conf " +
         "https://git.openstack.org/cgit/openstack/swift/plain/etc/container-reconciler.conf-sample?h=stable/kilo")

    sudo("curl -o /etc/swift/object-expirer.conf " +
         "https://git.openstack.org/cgit/openstack/swift/plain/etc/object-expirer.conf-sample?h=stable/kilo")

    sudo("mkdir -p /var/cache/swift")


@task
@parallel
def start_proxy(swift_cfg):
    put(remote_path='/etc/swift/swift.conf',
        local_path=StringIO(swift_cfg),
        use_sudo=True)
    sudo("chown -R swift:swift /etc/swift /var/cache/swift")

    sudo("systemctl enable openstack-swift-proxy.service memcached.service")
    sudo("systemctl start openstack-swift-proxy.service memcached.service")


@task
@parallel
def start_storage(swift_cfg):
    put(remote_path='/etc/swift/swift.conf',
        local_path=StringIO(swift_cfg),
        use_sudo=True)
    sudo("chown -R swift:swift /etc/swift /var/cache/swift")

    services = """
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

    services = " ".join(services.split())

    sudo("systemctl enable " + services)
    sudo("systemctl start " + services)


def finalize(nodes):
    url = "https://git.openstack.org/cgit/openstack/swift/plain/etc/swift.conf-sample?h=stable/kilo"
    swift_cfg = urllib2.urlopen(url).read()

    suff = hashlib.md5(str(uuid.uuid1())).hexdigest()
    swift_cfg = swift_cfg.replace("swift_hash_path_suffix = changeme",
                                  "swift_hash_path_suffix = " + suff)

    suff = hashlib.md5(str(uuid.uuid1())).hexdigest()
    swift_cfg = swift_cfg.replace("swift_hash_path_prefix = changeme",
                                  "swift_hash_path_prefix = " + suff)

    execute(start_proxy, swift_cfg, hosts=[proxy.ip for proxy in nodes.proxy])
    execute(start_storage, swift_cfg, hosts=[storage.ip for storage in nodes.storage])


if __name__ == "__main__":
    cmd, conf_path = sys.argv[1:]
    nodes, cfg = load_cfg(conf_path)

    env.user = 'root'

    if cmd == 'clear':
        pass
    else:
        execute(prepare, hosts=nodes.all_ip)

        execute(deploy_controller,
                hosts=[nodes.controler.ip])

        execute(deploy_storage, conf_path,
                hosts=[storage.ip for storage in nodes.storage])

        execute(setup_rings, conf_path,
                hosts=[nodes.controler.ip])

        finalize(nodes)

    disconnect_all()