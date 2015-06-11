

def clear(devs):
    ro = []
    for dev in devs:
        try:
            sudo("python -c 'open(\"{0}\", \"rb+\").write(chr(0) * 100)' ".format(dev))
        except:
            ro.append(dev)
    sudo("sync")
    return ro


READ_SZ = 20


@task
@parallel
def gather(devs_map):
    res = {}
    sudo("sync")
    for dev in devs_map[env.host]:
        res[dev] = sudo("python -c 'print open(\"{0}\", \"rb\").read({1})' ".format(dev), READ_SZ)
    return res


def fill(devs, node_name):
    res = {}
    for dev in devs:
        cmd = "python -c 'print open(\"{0}\", \"rb+\").write(\"{1}_{0}\")'"
        res[dev] = sudo(cmd.format(dev, node_name))
    sudo("sync")


def check_devs(nodes):
    per_node_devs = {}
    devs = execute(get_scsi_dev_mapping, hosts=nodes)
    for node, devs_str in devs.items():
        per_node_devs[node] = []
        for scsi_id, dev in devs_str.items():
            assert scsi_id[0] == '[' and scsi_id[-1] == ']' and scsi_id.count(":") == 3
            num_id = map(int, scsi_id[1:-1].split(":"))
            if num_id[0] == 1 and num_id[2] >= 3:
                per_node_devs[node].append(dev)

    for node in nodes:
        print execute(clear, per_node_devs[node], hosts=[node])

    first_gather = execute(gather, per_node_devs, hosts=[node])
    for host, dev_data in first_gather.items():
        for dev, data in dev_data:
            if data != "\x00" * READ_SZ:
                print host, dev, data

