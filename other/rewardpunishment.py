from other.buffer import RingBuffer


class RewardAndPunishment:
    """Reward and Punishment mechanism inspired by "Modeling and Assessing Quality of Information in Multisensor
    Multimedia Monitoring Systems" by Hoassain et al. """

    def __init__(self, window):
        self.window = window
        self.buffer = RingBuffer(window)
        self.reward = 1.0
        self.lowest = self.reward
        for j in range(0, window):
            self.update(True)
        self.started = False

    def update(self, truthhold):
        # detect if rp was already used
        self.started = True

        alpha_w_minus_1 = float(len(list(filter(lambda x: x == 1, self.buffer.items[1:]))))
        w_minus_1 = self.buffer.fill_level() - 1
        alpha = 0.0
        if truthhold != 0:
            alpha = 1.0
        self.buffer.add(alpha)

        if w_minus_1 > 0:
            r_p = (alpha_w_minus_1 / w_minus_1) - ((alpha_w_minus_1 + alpha) / (w_minus_1 + 1))
            self.reward -= 2 * r_p
            self.lowest = min(self.lowest, self.reward)
        else:
            if truthhold:
                self.reward = 1.0
            else:
                self.reward = 0.0

    def value(self):
        # return 'NA' if not used yet
        if not self.started:
            return 'NA'
        if self.reward < 0:
            return 0
        elif abs(self.reward) > 1:
            return 1
        return round(abs(self.reward), 2)  # * 2 - 1


if __name__ == "__main__":
    rp = RewardAndPunishment(5)
    print(rp.value())
    rp.update(0)
    print(rp.value())

    for i in range(0, 9):
        rp.update(0)
        print(rp.value())

    for i in range(0, 9):
        rp.update(0)
        print(rp.value())

