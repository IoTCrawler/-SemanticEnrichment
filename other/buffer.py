class RingBuffer(object):
    """docstring for RingBuffer"""

    def __init__(self, size):
        self.size = size
        self.items = []

    def add(self, item):
        self.items.append(item)
        if len(self.items) > self.size:
            del self.items[0]

    def min(self):
        return min(self.items)

    def max(self):
        return max(self.items)

    def fill_level(self):
        return len(self.items)

    def __iter__(self):
        return self.items.__iter__()


class NumericRingBuffer(RingBuffer):
    """docstring for NumericRingBuffer"""

    def __init__(self, size):
        super(NumericRingBuffer, self).__init__(size)
        self.counter = 0
        self.sum = 0.0

    def add(self, item):
        super(NumericRingBuffer, self).add(item)
        self.counter += 1
        self.sum += item

    def mean(self):
        return float(sum(self.items)) / self.size

    def mean_all(self):
        return self.sum / self.counter
