import collections

import matplotlib.pyplot as plt


data = []
for ln in open('/tmp/data1'):
    ln = ln.strip()
    if ln != "":
        data.append(ln)

assert len(data) % 5 == 0

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

RSMAP = [('K', 1024),
         ('M', 1024 ** 2),
         ('G', 1024 ** 3),
         ('T', 1024 ** 4)]


def b2ssize(size):
    if size < 1024:
        return str(size)

    for name, scale in RSMAP:
        if size < 1024 * scale:
            if size % scale == 0:
                return "{0}{1}".format(size // scale, name)
            else:
                return "{0:.1f}{1}".format(float(size) / scale, name)

    return "{0}{1}i".format(size // scale, name)


vals = collections.defaultdict(lambda: [])
for i in range(len(data) / 5):
    dt = data[i * 5: i * 5 + 5]
    vals[(dt[0], ssize2b(dt[1]))].append(map(int, dt[2:]))

ax = plt.subplot(111)
colors = ['b', 'g', 'r']

tick_labels = []
ticks = []
for x, (key, val) in enumerate(sorted(vals.items()), 1):
    legend = [[], []]
    ticks.append(x)
    tick_labels.append(key[0] + " " + b2ssize(key[1]))
    for add, cv in enumerate(sorted(val)):
        npos = x - 0.2 + 0.2 * add
        br = ax.bar(npos,
                    [cv[1]],
                    width=0.2,
                    color=colors[add],
                    align='center')
        legend[0].append(br[0])
        legend[1].append(str(cv[0]) + " threads")

ax.set_xticks(ticks)
ax.set_xticklabels(tick_labels)
ax.legend(*legend)
ax.set_ylabel('% diff')
plt.show()
