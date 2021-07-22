from package.api.Health import Health
from package.api.Traffic import Traffic


class City:
    def __init__(self, name):
        self.name = name
        self.covid_fatality = float()
        self.covid_incidence = float()
        self.quality_of_life = float()
        self.health_care = Health()
        self.traffic = Traffic()
        self.pollution = float()
        self.climate = float()

    def __repr__(self):
        return f"{self.name} {self.quality_of_life}"

    def __str__(self):
        return self.name

    def json(self):
        return {
            "name": self.name,
            "indexes": {
                "covid_fatality": self.covid_fatality,
                "covid_incidence": self.covid_incidence,
                "quality_of_life": self.quality_of_life,
                "health_care": self.health_care.json(),
                "traffic": self.traffic.json(),
                "pollution": self.pollution,
                "climate": self.climate
            }
        }

    def array(self):
        return [self.name, self.covid_fatality, self.covid_incidence,
                self.quality_of_life] + self.health_care.array() + self.traffic.array() + [self.pollution, self.climate]
