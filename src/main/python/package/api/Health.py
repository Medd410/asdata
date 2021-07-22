class Health:
    def __init__(self):
        self.index = float()
        self.skill = float()
        self.speed = float()
        self.equipment = float()
        self.accuracy = float()
        self.friendliness = float()
        self.responsiveness = float()
        self.cost = float()
        self.location = float()

    def json(self):
        return {
            "index": self.index,
            "skill": self.skill,
            "speed": self.speed,
            "equipment": self.equipment,
            "accuracy": self.accuracy,
            "friendliness": self.friendliness,
            "responsiveness": self.responsiveness,
            "cost": self.cost,
            "location": self.location
        }

    def array(self):
        return [self.index, self.skill, self.speed, self.equipment, self.accuracy, self.friendliness, self.responsiveness, self.cost, self.location]