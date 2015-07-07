import sys
import json
import texttable
import collections

import matplotlib.pyplot as plt

from statistic import data_property


class TestResults(object):
    def __init__(self, data):
        self.times = []
        self.iops = []


def load_file(fname):
    ips = []
    res = collections.defaultdict(lambda: [])
    skipped = 0
    for line in open(fname):
        if line.startswith('{'):
            data = json.loads(line)
            times = [ttime for ttime, _ in data.values()]

            ips.extend(data.keys())
            if min(times) * 1.5 < max(times):
                for _, node_res in data.values():
                    for (key, data) in node_res:
                        skipped += len(data)
                continue

            for _, node_res in data.values():
                for (key, data) in node_res:
                    res[tuple(key)].extend(data)

    print "Res size =", sum(len(v) for v in res.values()), "Skipped =", skipped
    return res, len(set(ips))


SMAP = dict(k=1024, m=1024 ** 2, g=1024 ** 3, t=1024 ** 4)


def ssize2b(ssize):
    try:
        if isinstance(ssize, (int, long)):
            return ssize

        ssize = ssize.lower()
        if ssize[-1] in SMAP:
            return int(ssize[:-1]) * SMAP[ssize[-1]]
        return int(ssize)
    except (ValueError, TypeError, AttributeError):
        raise ValueError("Unknow size format {0!r}".format(ssize))


def report(processed_data, keys, node_count):
    tab = texttable.Texttable(max_width=200)
    tab.set_deco(tab.HEADER | tab.VLINES | tab.BORDER)
    tab.set_cols_align(["l", "l", "r", "r", "r", "r"])

    pkey = None

    header = ["test", "size", "nthreads", "iops ~ conf", "lat ms ~ conf", "err"]
    tab.header(header)
    sep = ['-' * len(i) for i in header]

    for key in keys:
        if pkey is not None and pkey[:2] != key[:2]:
            tab.add_row(sep)

        pkey = key
        test, size, proc = key

        iops = processed_data[key]["iops"]
        lat = processed_data[key]["lat"]
        errs = processed_data[key]["errs"]

        row = [
            test, size, proc * node_count,
            "{0} ~ {1:>4}".format(int(iops.average), int(iops.confidence)),
            "{0} ~ {1:>4}".format(int(lat.average), int(lat.confidence)),
            "{0}".format(int(errs.average)),
        ]
        tab.add_row(row)

    return tab.draw()


def process_data(res, node_count):
    keys = res.keys()
    keys.sort(key=lambda x: (x[0], ssize2b(x[1]), x[2]))
    nres = {}

    for key in keys:
        nres[key] = dict(
            iops=data_property([i["iops"] * node_count for i in res[key]]),
            lat=data_property([i["lat"] * 1000 for i in res[key]]),
            errs=data_property([i["errs"] for i in res[key]])
        )

    return nres, keys


def io_chart(title, legend, marks, iops, iops_err, latv_50, latv_95):
    width = 0.35
    lc = len(marks)
    xt = range(1, lc + 1)

    fig, p1 = plt.subplots()
    xpos = [i - width / 2 for i in xt]

    p1.bar(xpos, iops,
           width=width,
           yerr=iops_err,
           ecolor='m',
           color='y',
           label=legend)

    p1.grid(True)
    handles1, labels1 = p1.get_legend_handles_labels()

    p2 = p1.twinx()
    p2.plot(xt, latv_50, label="lat med")

    if latv_95 is not None:
        p2.plot(xt, latv_95, label="lat 95%")

    plt.xlim(0.5, lc + 0.5)
    plt.xticks(xt, map(str, marks))
    p1.set_xlabel("Thread cumulative")
    p1.set_ylabel(legend)
    p2.set_ylabel("Latency ms")
    plt.title(title)
    handles2, labels2 = p2.get_legend_handles_labels()

    plt.legend(handles1 + handles2, labels1 + labels2,
               loc='center left', bbox_to_anchor=(1.1, 0.81))

    plt.subplots_adjust(right=0.68)
    plt.show()


def plot_data_over_time(raw_res, node_count):
    keys = raw_res.keys()
    assert len(keys) == 1

    iops = [data['iops'] for data in raw_res[keys[0]]]
    iops = map(sum, zip(iops[::3], iops[1::3], iops[2::3]))
    plt.plot(iops)
    plt.show()


if __name__ == "__main__":
    raw_res, node_count = load_file(sys.argv[1])
    # plot_data_over_time(raw_res, node_count)
    # exit(1)
    res, keys = process_data(raw_res, node_count)
    print report(res, keys, node_count)

    # iops = []
    # iops_err = []
    # lat = []
    # marks = []

    # ttype = 'get'
    # ssize = '64k'

    # for key in keys:
    #     test, size, th = key
    #     if test == ttype and size == ssize:
    #         io = res[key]['iops']
    #         iops.append(io.average)
    #         iops_err.append(io.confidence)
    #         lat.append(res[key]['lat'].average)
    #         marks.append(th * node_count)

    # io_chart("{0} {1}".format(ttype, ssize),
    #          "iops", marks, iops, iops_err, lat, None)
