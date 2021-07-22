class Traffic:
    def __init__(self):
        self.index = float()
        self.time = float()
        self.inefficiency = float()

    def json(self):
        return {
            "index": self.index,
            "time": self.time,
            "inefficiency": self.inefficiency
        }

    def array(self):
        return [self.index, self.time, self.inefficiency]
