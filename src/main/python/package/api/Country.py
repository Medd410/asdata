from package.api.City import City


class Country(City):
    def __init__(self, name):
        super().__init__(name=name)
        self.cities = []

    def json(self):
        data = super().json()
        data["cities"] = [city.json() for city in self.cities]
        return data

    def sort_cities(self):
        self.cities.sort(key=lambda city: city.name)
