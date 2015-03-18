from fabric.api import run, task, env
from influxdb import InfluxDBClient

from deploy_ceph import prepare_cmds


@task
def deploy_influxdb():
    commands = """
    wget http://s3.amazonaws.com/influxdb/influxdb_latest_amd64.deb

    sudo dpkg -i influxdb_latest_amd64.deb

    sudo service influxdb start
    """

    for cmd in prepare_cmds(commands):
        run(cmd)

    # conn = InfluxDBClient(env.host_string, 8086, 'root', 'root', '')
    # conn.create_database('perf')
    # conn.add_database_user('perf', 'perf', ('perf', 'perf'))
