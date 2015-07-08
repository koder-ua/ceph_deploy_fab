import math
import itertools

try:
    from scipy import stats
    no_numpy = False
except ImportError:
    no_numpy = True


def med_dev(vals):
    med = sum(vals) / len(vals)
    dev = ((sum(abs(med - i) ** 2.0 for i in vals) / len(vals)) ** 0.5)
    return med, dev


def round_3_digit(val):
    return round_deviation((val, val / 10.0))[0]


def round_deviation(med_dev):
    med, dev = med_dev

    if dev < 1E-7:
        return med_dev

    dev_div = 10.0 ** (math.floor(math.log10(dev)) - 1)
    dev = int(dev / dev_div) * dev_div
    med = int(med / dev_div) * dev_div
    return [type(med_dev[0])(med),
            type(med_dev[1])(dev)]


def groupby_globally(data, key_func):
    grouped = {}
    grouped_iter = itertools.groupby(data, key_func)

    for (bs, cache_tp, act, conc), curr_data_it in grouped_iter:
        key = (bs, cache_tp, act, conc)
        grouped.setdefault(key, []).extend(curr_data_it)

    return grouped


class StatProps(object):
    def __init__(self):
        self.average = None
        self.mediana = None
        self.perc_95 = None
        self.perc_5 = None
        self.deviation = None
        self.confidence = None
        self.min = None
        self.max = None
        self.raw = None

    def rounded_average_conf(self):
        return round_deviation((self.average, self.confidence))

    def rounded_average_dev(self):
        return round_deviation((self.average, self.deviation))

    def __str__(self):
        return "StatProps({0} ~ {1})".format(round_3_digit(self.average),
                                             round_3_digit(self.deviation))

    def __repr__(self):
        return str(self)


def data_property(data, confidence=0.95):
    res = StatProps()
    if len(data) == 0:
        return res

    data = sorted(data)
    res.average, res.deviation = med_dev(data)
    res.max = data[-1]
    res.min = data[0]

    ln = len(data)
    if ln % 2 == 0:
        res.mediana = (data[ln / 2] + data[ln / 2 - 1]) / 2
    else:
        res.mediana = data[ln / 2]

    res.perc_95 = data[int((ln - 1) * 0.95)]
    res.perc_5 = data[int((ln - 1) * 0.05)]

    if not no_numpy and ln >= 3:
        res.confidence = stats.sem(data) * \
                         stats.t.ppf((1 + confidence) / 2, ln - 1)
    else:
        res.confidence = res.deviation

    res.raw = data[:]
    return res
