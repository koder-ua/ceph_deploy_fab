import re
import sys
import time
import json
import collections

import yaml
# import texttable

from fabric.api import run, task, sudo
from fabric.network import disconnect_all
from fabric.context_managers import hide
from fabric.api import parallel, env, execute


@task
@parallel
def run_tests(cmds, size, runtime, procs):
    cont = env.host.replace(".", '-')
    cmd = 'source swiftrc ; cd getput ;'
    cmd += './getput -c {cont} --obj test --size "{size}" --tests "{cmds}" '
    cmd += '--runtime {runtime} --proxies $SW_NODES --procs {procs} --preauthtoken $SW_TOKEN'

    with hide('stdout', 'stderr'):
        t = time.time()
        print "Node ", env.host, "start test at",  t
        res = run(cmd.format(cont=cont, cmds=cmds, size=size, runtime=runtime, procs=procs))
        print "Node ", env.host, "finish test in",  int(time.time() - t)

    return res


def start_collect_data():
    devs = []
    sudo("yum -y install screen")

    for line in run('mount').split("\n"):
        if '/srv/node' in line:
            devs.append(line.strip().split(" ")[0])


def process(data):
    rr_fl = r"\d+\.?\d*"
    rr_dict = [("rank", r"\d+", int),
               ("test", r"\w+", str),
               ("clts", r"\d+", int),
               ("proc", r"\d+", int),
               ("size", r"\d+[kmg]", str),
               ("start", r"\d\d:\d\d:\d\d", str),
               ("end", r"\d\d:\d\d:\d\d", str),
               ("bw", rr_fl, float),
               ("io", r"\d+", int),
               ("iops", rr_fl, float),
               ("ppspsec", rr_fl, float),
               ("errs", r"\d+", int),
               ("lat", rr_fl, float),
               ("median", rr_fl, float),
               ("lat_range", rr_fl + '-' + rr_fl, lambda x: (float(x.split('-')[0]), float(x.split('-')[1]))),
               ("cpu", rr_fl, float),
               ('comp', r'\w+', str)]

    rr_str = ""
    types = {}
    for name, rr, tp in rr_dict:
        rr_str += r"\s*(?P<{0}>{1})\s*".format(name, rr)
        types[name] = tp

    rr = re.compile(rr_str + "$")
    res = collections.defaultdict(lambda: [])

    val_keys = ("bw", "iops", "lat", "median", "lat_range", "cpu", "errs")
    key_keys = ("test", "size", "proc")

    for line in data.split("\n"):
        r = rr.match(line)
        if r is not None:
            key = tuple(types[kname](r.group(kname)) for kname in key_keys)
            vls_it = (types[kname](r.group(kname)) for kname in val_keys)
            val = dict(zip(val_keys, vls_it))
            res[key].append(val)
    return dict(res.items())


def report(res):
    import texttable

    keys = res.keys()
    keys.sort(key=lambda x: (x[0], x[2], x[1]))
    tab = texttable.Texttable(max_width=200)
    tab.set_deco(tab.HEADER | tab.VLINES | tab.BORDER)
    tab.set_cols_align(["l", "l", "r", "r", "r", "r"])

    avg = lambda x: sum(map(float, x)) / len(x)

    for key in keys:
        test, size, proc = key
        row = [
            test, size, proc,
            "{0:.1f}".format(round(avg([i["iops"] for i in res[key]]), 1)),
            int(avg([i["lat"] for i in res[key]]) * 1000),
            int(avg([i["lat"] for i in res[key]]))
        ]
        tab.add_row(row)

    tab.header(["test", "size", "nthreads", "iops", "lat", "err"])

    return tab.draw()


if __name__ == "__main__":
    cfg = yaml.load(open(sys.argv[1]).read())
    test_nodes = [ip.strip() for ip in cfg['testnodes']]

    env.user = 'root'

    all_res = []
    for test in cfg['tests']:
        test_res = collections.defaultdict(lambda: [])
        for size in test['sizes']:
            for procs in test['procs']:
                res = execute(run_tests,
                              cmds=test['cmds'],
                              size=size,
                              runtime=test['runtime'],
                              procs=procs,
                              hosts=test_nodes)

                for key, val in res.items():
                    test_res[key].append(process(val).items())
        all_res.append(test_res)

    print json.dumps(all_res)
    disconnect_all()

