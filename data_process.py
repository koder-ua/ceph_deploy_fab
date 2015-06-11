import re
import sys
import texttable
import collections

# Rank Test  Clts Proc  OSize  Start     End        MB/Sec   Ops   Ops/Sec Errs Latency  Median    LatRange   %CPU  Comp

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

for line in open(sys.argv[1]):
    r = rr.match(line)
    if r is not None:
        key = tuple(types[kname](r.group(kname)) for kname in key_keys)
        vls_it = (types[kname](r.group(kname)) for kname in val_keys)
        val = dict(zip(val_keys, vls_it))
        res[key].append(val)


def report(res):
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

print report(res)

# def plot(iops, lats):
#     labels_and_data_mp = collections.defaultdict(lambda: [])
#     vls = {}

#     # plot io_time = func(bsize)
#     for res in processed_results.values():
#         if res.name.startswith('linearity_test'):
#             iotimes = [1000. / val for val in res.iops.raw]

#             op_summ = get_test_summary(res.params)[:3]

#             labels_and_data_mp[op_summ].append(
#                 [res.p.blocksize, res.iops.raw, iotimes])

#             cvls = res.params.vals.copy()
#             del cvls['blocksize']
#             del cvls['rw']

#             cvls.pop('sync', None)
#             cvls.pop('direct', None)
#             cvls.pop('buffered', None)

#             if op_summ not in vls:
#                 vls[op_summ] = cvls
#             else:
#                 assert cvls == vls[op_summ]

#     all_labels = None
#     _, ax1 = plt.subplots()
#     for name, labels_and_data in labels_and_data_mp.items():
#         labels_and_data.sort(key=lambda x: ssize2b(x[0]))

#         labels, _, iotimes = zip(*labels_and_data)

#         if all_labels is None:
#             all_labels = labels
#         else:
#             assert all_labels == labels

#         plt.boxplot(iotimes)
#         if len(labels_and_data) > 2 and \
#            ssize2b(labels_and_data[-2][0]) >= 4096:

#             xt = range(1, len(labels) + 1)

#             def io_time(sz, bw, initial_lat):
#                 return sz / bw + initial_lat

#             x = numpy.array(map(ssize2b, labels))
#             y = numpy.array([sum(dt) / len(dt) for dt in iotimes])
#             popt, _ = scipy.optimize.curve_fit(io_time, x, y, p0=(100., 1.))

#             y1 = io_time(x, *popt)
#             plt.plot(xt, y1, linestyle='--',
#                      label=name + ' LS linear approx')

#             for idx, (sz, _, _) in enumerate(labels_and_data):
#                 if ssize2b(sz) >= 4096:
#                     break

#             bw = (x[-1] - x[idx]) / (y[-1] - y[idx])
#             lat = y[-1] - x[-1] / bw
#             y2 = io_time(x, bw, lat)
#             plt.plot(xt, y2, linestyle='--',
#                      label=abbv_name_to_full(name) +
#                      ' (4k & max) linear approx')

#     plt.setp(ax1, xticklabels=labels)

#     plt.xlabel("Block size")
#     plt.ylabel("IO time, ms")

#     plt.subplots_adjust(top=0.85)
#     plt.legend(bbox_to_anchor=(0.5, 1.15),
#                loc='upper center',
#                prop={'size': 10}, ncol=2)
#     plt.grid()
#     iotime_plot = get_emb_data_svg(plt)
#     plt.clf()

#     # plot IOPS = func(bsize)
#     _, ax1 = plt.subplots()

#     for name, labels_and_data in labels_and_data_mp.items():
#         labels_and_data.sort(key=lambda x: ssize2b(x[0]))
#         _, data, _ = zip(*labels_and_data)
#         plt.boxplot(data)
#         avg = [float(sum(arr)) / len(arr) for arr in data]
#         xt = range(1, len(data) + 1)
#         plt.plot(xt, avg, linestyle='--',
#                  label=abbv_name_to_full(name) + " avg")

#     plt.setp(ax1, xticklabels=labels)
#     plt.xlabel("Block size")
#     plt.ylabel("IOPS")
#     plt.legend(bbox_to_anchor=(0.5, 1.15),
#                loc='upper center',
#                prop={'size': 10}, ncol=2)
#     plt.grid()
#     plt.subplots_adjust(top=0.85)

#     iops_plot = get_emb_data_svg(plt)

#     res = set(get_test_lcheck_params(res) for res in processed_results.values())
#     ncount = list(set(res.testnodes_count for res in processed_results.values()))
#     conc = list(set(res.concurence for res in processed_results.values()))

#     assert len(conc) == 1
#     assert len(ncount) == 1

#     descr = {
#         'vm_count': ncount[0],
#         'concurence': conc[0],
#         'oper_descr': ", ".join(res).capitalize()
#     }

#     params_map = {'iotime_vs_size': iotime_plot,
#                   'iops_vs_size': iops_plot,
#                   'descr': descr}

#     return get_template('report_linearity.html').format(**params_map)
