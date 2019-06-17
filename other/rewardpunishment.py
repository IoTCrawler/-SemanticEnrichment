from other.buffer import RingBuffer


class RewardAndPunishment:
    """Reward and Punishment mechanism inspired by "Modeling and Assessing Quality of Information in Multisensor
    Multimedia Monitoring Systems" by Hoassain et al. """
    

    def __init__(self, window):
        self.window = window
        self.buffer = RingBuffer(window)
        self.reward = 1.0
        self.lowest = self.reward
        for i in range(0, window):
            self.update(True)

    def update(self, truthhold):
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
        if abs(self.reward) < 0:
            return 0
        elif abs(self.reward) > 1:
            return 1
        return abs(self.reward)  # * 2 - 1




