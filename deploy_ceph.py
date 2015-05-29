import re
import json
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


rpm_repo = """[ceph-noarch]
name=Ceph noarch packages
baseurl=http://ceph.com/rpm-{ceph_release}/{release}/noarch
enabled=1
gpgcheck=1
type=rpm-md
gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc
priority=1

[ceph]
name=Ceph packages
baseurl=http://ceph.com/rpm-{ceph_release}/{release}/x86_64
enabled=1
gpgcheck=1
type=rpm-md
gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc
priority=1

[ceph-source]
name=Ceph source packages
baseurl=http://ceph.com/rpm-{ceph_release}/{release}/SRPMS/
enabled=1
gpgcheck=1
type=rpm-md
gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc
priority=1
"""


def get_distro():
    if exists("/etc/redhat-release"):
        return 'rh'
    return 'ubuntu'


def prepare_node(ceph_release):
    if 'rh' == get_distro():
        repo_fc = rpm_repo.format(release='el7', ceph_release=ceph_release)

        if not exists('/etc/ceph'):
            sudo("mkdir /etc/ceph")

        put(remote_path='/etc/yum.repos.d/ceph.repo',
            local_path=StringIO(repo_fc),
            use_sudo=True)

        with hide('stdout', 'stderr'):
            sudo("yum -y install epel-release")
            sudo("yum -y update")
            sudo("yum -y install yum-plugin-priorities ntp ntpdate ntp-doc ceph")
    else:
        add_ceph_dev_repo_keys = "wget -q -O- " + \
                                 "'https://ceph.com/git/?p=ceph.git" + \
                                 ";a=blob_plain;f=keys/release.asc' " + \
                                 "| apt-key add -"
        sudo(add_ceph_dev_repo_keys)

        add_ceph_dev_repo_templ = "echo deb http://ceph.com/" + \
                                  "debian-{0}/ " + \
                                  "$(lsb_release -sc) main | tee /etc/apt/" + \
                                  "sources.list.d/ceph.list"
        sudo(add_ceph_dev_repo_templ.format(ceph_release))

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
def clear_mon(conf_path=deployment_config):
    pass


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

    if exists(params.monmap_path):
        sudo("rm -f {0}".format(params.monmap_path))

    if exists(params.mon_keyring_path):
        sudo("rm -f {0}".format(params.mon_keyring_path))

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

        sudo chmod a+r {0.admin_keyring_path}"""

    if 'rh' == get_distro():
        commands_templ += "\n\nsudo touch /var/lib/ceph/mon/{0.clustername}-{0.hostname}/sysvinit" + \
                          "\n\nsudo /etc/init.d/ceph start mon.{0.hostname}"""
    else:
        commands_templ += "\n\nsudo start ceph-mon id={0.hostname}"

    commands_templ += """\n\nceph osd lspools\n\nceph -s"""

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


# @task
# @parallel
# def add_new_osd(mon_ip, conf_path=deployment_config):
#     params = get_config(conf_path)
#     params.osd_uuid = str(uuid.uuid4())
#     params.mon_ip = mon_ip

#     # executed on mon host

#     keys = {'fsid': 'fsid_uuid', 'mon initial members': 'mons'}

#     res = execute(allocate_osd_id_and_read_config, params, keys,
#                   hosts=[params.mon_ip])

#     for attr, val in res[params.mon_ip].items():
#         setattr(params, attr, val)

#     # executed on osd host
#     prepare_node(params.ceph_release)

#     # put ceph config
#     osd_config_file = ceph_config_file_templ.format(params)

#     put(remote_path=params.ceph_cfg_path,
#         local_path=StringIO(osd_config_file),
#         use_sudo=True)

#     # put admin keyring
#     put(remote_path=params.admin_keyring_path,
#         local_path=StringIO(params.admin_keyring_content),
#         use_sudo=True)

#     params.data_mount_path = params.data_mount_path.format(params)

#     commands_templ = """
#     sudo mkdir -p {0.data_mount_path}

#     sudo mkfs -t {0.fs_type} {0.osd_data_dev}

#     sudo mount {0.mount_opst} {0.osd_data_dev} {0.data_mount_path}

#     sudo ceph-osd -c {0.ceph_cfg_path} -i {0.osd_num}
#         --cluster {0.clustername} --mkfs --mkkey --osd-uuid {0.osd_uuid}

#     sudo ceph auth add osd.{0.osd_num} osd 'allow *' mon 'allow profile osd' -i
#         /var/lib/ceph/osd/{0.clustername}-{0.osd_num}/keyring

#     ceph osd crush add-bucket {0.hostname} host

#     ceph osd crush move {0.hostname} root=default
#     """

#     if 'rh' == get_distro():
#         # "\n\nsudo touch /var/lib/ceph/mon/{0.clustername}-{0.hostname}/sysvinit"
#         commands_templ += "\n\nsudo /etc/init.d/ceph start osd.{0.hostname}"""
#     else:
#         commands_templ += "\n\nsudo start ceph-osd id={0.osd_num}"

#     commands_templ += """
#     ceph osd crush add {0.osd_num} {0.osd_weigth} host={0.hostname}

#     ceph -s
#     """

#     for cmd in prepare_cmds(commands_templ.format(params)):
#         run(cmd)


@task
@parallel
def netapp_add_new_osd(mon_ip, conf_path=deployment_config):
    params = get_config(conf_path)
    assert params.fs_type == 'xfs'
    params.mon_ip = mon_ip

    # executed on mon host

    osd_devs = run("ls -1 /dev/sd*").split()
    # select HDD devices
    osd_devs = [dev for dev in osd_devs if len(os.path.basename(dev)) == 4 and not dev[-1].isdigit()]
    print osd_devs
    return

    #osd_devs = ['/dev/sda3', '/dev/sdb', '/dev/sdc']

    # executed on osd host
    prepare_node(params.ceph_release)

    if not exists(params.ceph_cfg_path):
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

    run("ceph osd crush add-bucket {0} host".format(params.hostname))
    run("ceph osd crush move {0} root=default".format(params.hostname))

    commands_templ = """
    sudo mkdir -p {0.data_mount_path}

    sudo mkfs.xfs -f {0.osd_data_dev}

    sudo mount {0.mount_opst} {0.osd_data_dev} {0.data_mount_path}

    sudo ceph-osd -c {0.ceph_cfg_path} -i {0.osd_num}
        --cluster {0.clustername} --mkfs --mkkey --osd-uuid {0.osd_uuid}

    sudo ceph auth add osd.{0.osd_num} osd 'allow *' mon 'allow profile osd' -i
        /var/lib/ceph/osd/{0.clustername}-{0.osd_num}/keyring
    """

    if 'rh' == get_distro():
        commands_templ += "\n\nsudo touch /var/lib/ceph/osd/{0.clustername}-{0.osd_num}/sysvinit"
        commands_templ += "\n\nsudo /etc/init.d/ceph start osd.{0.osd_num}"
    else:
        commands_templ += "\n\nsudo start ceph-osd id={0.osd_num}"

    commands_templ += """\n
    ceph osd crush add {0.osd_num} {0.osd_weigth} host={0.hostname}

    ceph -s
    """

    keys = {'fsid': 'fsid_uuid', 'mon initial members': 'mons'}

    mp_templ = params.data_mount_path
    for dev in osd_devs:
        params.osd_uuid = str(uuid.uuid4())

        res = execute(allocate_osd_id_and_read_config, params, keys,
                      hosts=[params.mon_ip])

        for attr, val in res[params.mon_ip].items():
            setattr(params, attr, val)
        params.osd_data_dev = dev
        params.data_mount_path = mp_templ.format(params)

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
def radosgw_centos(conf_path=deployment_config):
    sudo("yum -y install httpd mod_ssl openssl")
    name = run("hostname -f")

    text = "ServerName " + name + "\n"
    text += "<IfModule !proxy_fcgi_module>\n"
    text += "LoadModule proxy_fcgi_module modules/mod_proxy_fcgi.so\n"
    text += "</IfModule>\n"

    append("/etc/httpd/conf/httpd.conf", text, use_sudo=True)
    run("openssl genrsa -out ca.key 2048")
    run('openssl req -new -key ca.key -out ca.csr -subj "/C=US/ST=LA/L=LA/O=Mirantis/OU=IT Department/CN=mirantis.com"')
    run("openssl x509 -req -days 365 -in ca.csr -signkey ca.key -out ca.crt")

    if not exists("/etc/pki/tls"):
        sudo("mkdir -p /etc/pki/tls")

    sudo("cp ca.crt /etc/pki/tls/certs")
    sudo("cp ca.key /etc/pki/tls/private/ca.key")
    sudo("cp ca.csr /etc/pki/tls/private/ca.csr")

    fd = StringIO()
    get("/etc/httpd/conf.d/ssl.conf", fd)
    res = []

    for line in fd.getvalue().split("\n"):
        if line.strip().startswith('SSLCertificateFile'):
            res.append("SSLCertificateFile /etc/pki/tls/certs/ca.crt")
        elif line.strip().startswith('SSLCertificateKeyFile'):
            res.append("SSLCertificateKeyFile /etc/pki/tls/private/ca.key")
        else:
            res.append(line)

    put(remote_path="/etc/httpd/conf.d/ssl.conf",
        local_path=StringIO("\n".join(res)), use_sudo=True)

    sudo("systemctl start httpd")
    sudo("yum -y install ceph-radosgw radosgw-agent")

    cmds = """
    sudo ceph-authtool --create-keyring /etc/ceph/ceph.client.radosgw.keyring

    sudo chmod +r /etc/ceph/ceph.client.radosgw.keyring

    sudo ceph-authtool /etc/ceph/ceph.client.radosgw.keyring -n client.radosgw.gateway --gen-key

    sudo ceph-authtool -n client.radosgw.gateway --cap osd 'allow rwx' --cap mon 'allow rwx' /etc/ceph/ceph.client.radosgw.keyring

    sudo ceph -k /etc/ceph/ceph.client.admin.keyring auth add client.radosgw.gateway -i /etc/ceph/ceph.client.radosgw.keyring
    """

    # sudo chown apache:apache /var/log/radosgw/client.radosgw.gateway.log
    # Distribute the keyring to the node with the gateway instance.

    for cmd in prepare_cmds(cmds):
        run(cmd)

    cfg = """
    [client.radosgw.gateway]
    host = {hostname}
    keyring = /etc/ceph/ceph.client.radosgw.keyring
    rgw socket path = /var/run/ceph/ceph.radosgw.gateway.fastcgi.sock
    log file = /var/log/radosgw/client.radosgw.gateway.log
    rgw print continue = false
    """.format(hostname=name).replace("\n    ", "\n")

    append("/etc/ceph/ceph.conf", cfg)

    sudo("mkdir -p /var/lib/ceph/radosgw/ceph-radosgw.gateway")
    sudo("chown apache:apache /var/run/ceph")

    # https://www.shell-tips.com/2014/09/08/sudo-sorry-you-must-have-a-tty-to-run-sudo/
    sudo("/etc/init.d/ceph-radosgw start")

    # update config on all nodes
    # Copy ceph.client.admin.keyring from admin node to gateway host

    cfg2 = """
    <VirtualHost *:80>
    ServerName localhost
    DocumentRoot /var/www/html

    ErrorLog /var/log/httpd/rgw_error.log
    CustomLog /var/log/httpd/rgw_access.log combined

    # LogLevel debug

    RewriteEngine On

    RewriteRule .* - [E=HTTP_AUTHORIZATION:%{HTTP:Authorization},L]

    SetEnv proxy-nokeepalive 1

    ProxyPass / unix:///var/run/ceph/ceph.radosgw.gateway.fastcgi.sock|fcgi://localhost:9000/

    </VirtualHost>"""

    put(remote_path="/etc/httpd/conf.d/rgw.conf",
        local_path=StringIO(cfg2), use_sudo=True)

    sudo("systemctl restart httpd")
    sudo('radosgw-admin user create --uid="testuser" --display-name="testuser"')
    sudo('radosgw-admin subuser create --uid=testuser --subuser=testuser:swift')
    swift_key = sudo("radosgw-admin key create --subuser=testuser:swift --key-type=swift --gen-secret")
    data = json.loads(swift_key)
    key = data['swift_keys'][0]["secret_key"]

    sudo("yum -y install python-setuptools")
    sudo("easy_install pip")
    sudo("pip install --upgrade setuptools")
    sudo("pip install --upgrade python-swiftclient")
    run("swift -A http://{0}/auth/1.0 -U testuser:swift -K '{1}' list".format(name, key))


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
