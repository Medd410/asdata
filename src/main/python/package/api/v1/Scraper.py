import time
import json

import aiohttp
import asyncio
import pandas as pd
from bs4 import BeautifulSoup

from package.api.constants import *
from package.api.Country import Country
from package.api.City import City


class Scraper:
    def __init__(self):
        self.countries = []
        self.df = pd.read_csv("01-01-2021.csv")
        self.sem = asyncio.Semaphore(8)

    async def get_countries_name(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(QUALITY_OF_LIFE_URL) as resp:
                assert resp.status == 200
                content = await resp.text()
                soup = BeautifulSoup(content, "html.parser")
                names = [a.get_text() for a in soup.find(class_="related_links").find_all("a")]
                return names

    # async def get_countries_population(self):
    #    async with aiohttp.ClientSession() as session:
    #        async with session.get(COUNTRY_POPULATION_URL) as resp:
    #            assert resp.status == 200
    #            content = await resp.text()
    #            soup = BeautifulSoup(content, "html.parser")
    #            for row in soup.find("tbody").find_all("tr"):
    #                data = [x.get_text() for x in row.find_all("td")]
    #                Country.population[data[1]] = int(data[2].replace(',', ''))

    # async def get_cities_population(self):
    #    async with aiohttp.ClientSession() as session:
    #        async with session.get(CITY_POPULATION_URL) as resp:
    #            assert resp.status == 200
    #            content = await resp.text()
    #            soup = BeautifulSoup(content, "html.parser")
    #            for row in soup.find("tbody").find_all("tr"):
    #                data = [x.get_text() for x in row.find_all("td")]
    #                City.population[data[1]] = int(data[3].replace(',', ''))

    async def get_countries_covid(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(COUNTRY_COVID_URL) as resp:
                assert resp.status == 200
                content = await resp.text()
                soup = BeautifulSoup(content, "html.parser")
                for row in soup.find(id="main_table_countries_today").find("tbody").find_all("tr"):
                    data = [x.get_text() for x in row.find_all("td")]
                    print(data)

    async def get_cities_name(self, country_name):
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{CITY_SEARCH_URL}?term={country_name}") as resp:
                assert resp.status == 200
                cities = await resp.json()
                names = [city.get("label").rsplit(",", 1)[0] for city in cities if
                         city.get("label").split(",")[1].strip() == country_name]
                return names

    def get_indexes(self, content):
        soup = BeautifulSoup(content, "html.parser")
        indexes_table = soup.find("table", attrs={"style": None, "class": None})
        indexes = [i.get_text().strip() for i in indexes_table.find_all("td", attrs={"style": "text-align: right"})]
        indexes = [None if "?" in i else float(i) for i in indexes]
        return indexes

    async def get_health_care_indexes(self, content):
        soup = BeautifulSoup(content, "html.parser")
        url = soup.find("a", attrs={"class": "discreet_link"}, string="Health Care Index", href=True)["href"]
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                assert resp.status == 200
                content = await resp.text()
                soup = BeautifulSoup(content, "html.parser")
                indexes_table = soup.find("table", attrs={"class": "data_wide_table"})
                indexes = [i.get_text().strip().split('\n')[0] for i in
                           indexes_table.find_all("td", attrs={"class": "indexValueTd"})]
                main_index = soup.find("table", attrs={"class": "table_indices"}).find("td", attrs={
                    "style": "text-align: right"}).get_text().strip()
                indexes.insert(0, main_index)
                indexes = [float(i) for i in indexes]
                return indexes

    async def get_traffic_indexes(self, content):
        soup = BeautifulSoup(content, "html.parser")
        url = soup.find("a", attrs={"class": "discreet_link"}, string="Traffic Commute Time Index", href=True)["href"]
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                assert resp.status == 200
                content = await resp.text()
                soup = BeautifulSoup(content, "html.parser")
                indexes_table = soup.find("table", attrs={"class": "table_indices"})
                indexes = [i.get_text().strip().replace(',', '') for i in
                           indexes_table.find_all("td", attrs={"style": "text-align: right"})]
                indexes = [float(i) for i in indexes]
                return indexes

    async def get_city_indexes(self, country_name, city_name):
        async with aiohttp.ClientSession() as session:
            city_url = f"{QUALITY_OF_LIFE_URL}city_result.jsp?country={country_name.replace(' ', '+')}" \
                       f"&city={city_name.replace(' ', '+')}"
            async with session.get(city_url) as resp:
                assert resp.status == 200
                content = await resp.text()
                resp_url = str(resp.url)
                if QUALITY_OF_LIFE_URL not in resp_url:
                    return [], None
                if "There are no  data for" in content:
                    return [], None
                if "error_message" in content:
                    return [], None
                indexes = self.get_indexes(content=content)
                return indexes, str(resp.real_url)

    async def get_country_indexes(self, country_name, cities=True, strict=True):
        async with self.sem:
            async with aiohttp.ClientSession() as session:
                country_url = f"{QUALITY_OF_LIFE_URL}country_result.jsp?country={country_name.replace(' ', '+')}"
                async with session.get(country_url) as resp:
                    assert resp.status == 200
                    print(f"Fetching country : {country_name}")
                    content = await resp.text()
                    country_indexes = self.get_indexes(content=content)
                    if strict and None in country_indexes:
                        return
                    country = Country(name=country_name)
                    country.quality_of_life = country_indexes[8]
                    country_health_indexes = await self.get_health_care_indexes(content=content)
                    country.health_care.index = country_health_indexes[0]
                    country.health_care.skill_and_competency = country_health_indexes[1]
                    country.health_care.speed = country_health_indexes[2]
                    country.health_care.modern_equipment = country_health_indexes[3]
                    country.health_care.accuracy_and_completeness = country_health_indexes[4]
                    country.health_care.friendliness_and_courtesy = country_health_indexes[5]
                    country.health_care.responsiveness_waitings = country_health_indexes[6]
                    country.health_care.cost = country_health_indexes[7]
                    country.health_care.location = country_health_indexes[8]
                    country_traffic_indexes = await self.get_traffic_indexes(content=content)
                    country.traffic.index = country_traffic_indexes[0]
                    country.traffic.time = country_traffic_indexes[1]
                    country.traffic.inefficiency = country_traffic_indexes[3]
                    country.pollution = country_indexes[7]
                    country.climate = country_indexes[3]
                    self.countries.append(country)
                    if not cities:
                        return
                    cities_name = await self.get_cities_name(country.name)
                    for city_name in cities_name:
                        city_indexes, city_url = await self.get_city_indexes(country_name=country.name,
                                                                             city_name=city_name)
                        if not city_indexes or not city_url or strict and None in city_indexes:
                            continue
                        async with session.get(city_url) as resp:
                            assert resp.status == 200
                            content = await resp.text()
                            city = City(name=city_name)
                            city.quality_of_life = city_indexes[8]
                            try:
                                city_health_indexes = await self.get_health_care_indexes(content=content)
                            except:
                                return
                            city.health_care.index = city_health_indexes[0]
                            city.health_care.skill_and_competency = city_health_indexes[1]
                            city.health_care.speed = city_health_indexes[2]
                            city.health_care.modern_equipment = city_health_indexes[3]
                            city.health_care.accuracy_and_completeness = city_health_indexes[4]
                            city.health_care.friendliness_and_courtesy = city_health_indexes[5]
                            city.health_care.responsiveness_waitings = city_health_indexes[6]
                            city.health_care.cost = city_health_indexes[7]
                            city.health_care.location = city_health_indexes[8]
                            try:
                                city_traffic_indexes = await self.get_traffic_indexes(content=content)
                            except:
                                return
                            city.traffic.index = city_traffic_indexes[0]
                            city.traffic.time = city_traffic_indexes[1]
                            city.traffic.inefficiency = city_traffic_indexes[3]
                            city.pollution = city_indexes[7]
                            city.climate = city_indexes[3]
                            country.cities.append(city)
                    country.sort_cities()

    def save_data(self, path):
        with open(path, "w") as f:
            json.dump(self.json(), f, indent=4, ensure_ascii=False)

    def load_data(self, path):
        self.countries.clear()
        with open(path, "r") as f:
            data = json.load(f)
            data_countries = data.get("countries")
            for data_country in data_countries:
                data_indexes = data_country.get("indexes")
                country = Country(name=data_country.get("name"))
                country.quality_of_life = data_indexes.get("quality_of_life")
                data_health_care = data_indexes.get("health_care")
                country.health_care.index = data_health_care.get("index")
                country.health_care.skill = data_health_care.get("skill")
                country.health_care.speed = data_health_care.get("speed")
                country.health_care.equipment = data_health_care.get("equipment")
                country.health_care.accuracy = data_health_care.get("accuracy")
                country.health_care.friendliness = data_health_care.get("friendliness")
                country.health_care.responsiveness = data_health_care.get("responsiveness")
                country.health_care.cost = data_health_care.get("cost")
                country.health_care.location = data_health_care.get("location")
                data_traffic = data_indexes.get("traffic")
                country.traffic.index = data_traffic.get("index")
                country.traffic.time = data_traffic.get("time")
                country.traffic.inefficiency = data_traffic.get("inefficiency")
                country.pollution = data_indexes.get("pollution")
                country.climate = data_indexes.get("climate")
                self.countries.append(country)
                data_cities = data_country.get("cities")
                for data_city in data_cities:
                    data_indexes = data_city.get("indexes")
                    city = City(name=data_city.get("name"))
                    city.quality_of_life = data_indexes.get("quality_of_life")
                    data_health_care = data_indexes.get("health_care")
                    city.health_care.index = data_health_care.get("index")
                    city.health_care.skill = data_health_care.get("skill")
                    city.health_care.speed = data_health_care.get("speed")
                    city.health_care.equipment = data_health_care.get("equipment")
                    city.health_care.accuracy = data_health_care.get("accuracy")
                    city.health_care.friendliness = data_health_care.get("friendliness")
                    city.health_care.responsiveness = data_health_care.get("responsiveness")
                    city.health_care.cost = data_health_care.get("cost")
                    city.health_care.location = data_health_care.get("location")
                    data_traffic = data_indexes.get("traffic")
                    city.traffic.index = data_traffic.get("index")
                    city.traffic.time = data_traffic.get("time")
                    city.traffic.inefficiency = data_traffic.get("inefficiency")
                    city.pollution = data_indexes.get("pollution")
                    city.climate = data_indexes.get("climate")
                    country.cities.append(city)

    async def get_countries_data(self):
        country_names = (await self.get_countries_name())
        tasks = [self.get_country_indexes(country_name) for country_name in country_names]
        await asyncio.wait(tasks)
        self.sort_countries()

    def scrape_data(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.get_countries_data())

    def json(self):
        return {
            "countries": [country.json() for country in self.countries]
        }

    def csv(self):
        pass

    def sort_countries(self):
        self.countries.sort(key=lambda country: country.name)


if __name__ == "__main__":
    t = time.perf_counter()
    scraper = Scraper()
    scraper.load_data("test_full.json")
    for country in scraper.countries:
        df = scraper.df.loc[
            (scraper.df["Country_Region"] == country.name) & (pd.isna(scraper.df["Province_State"])), ["Incident_Rate",
                                                                                                       "Case_Fatality_Ratio"]]
        try:
            country.covid_incidence = round(df["Incident_Rate"].values[0], 2)
            country.covid_fatality = round(df["Case_Fatality_Ratio"].values[0], 2)
        except:
            pass
        for city in country.cities:
            df = scraper.df.loc[
                (scraper.df["Province_State"] == city.name) & (scraper.df["Country_Region"] == country.name), [
                    "Incident_Rate", "Case_Fatality_Ratio"]]
            try:
                city.covid_incidence = round(df["Incident_Rate"].values[0], 2)
                city.covid_fatality = round(df["Case_Fatality_Ratio"].values[0], 2)
            except:
                pass
    final_csv = {
        "Name": [],
        "Fatality": [],
        "Incidence": [],
        "Quality of Life": [],
        "Health Care": [],
        "Skill": [],
        "Speed": [],
        "Equipment": [],
        "Accuracy": [],
        "Friendliness": [],
        "Responsiveness": [],
        "Cost": [],
        "Location": [],
        "Traffic": [],
        "Time": [],
        "Inefficiency": [],
        "Pollution": [],
        "Climate": []
    }
    for country in scraper.countries:
        final_csv["Name"].append(country.name)
        final_csv["Fatality"].append(country.covid_fatality)
        final_csv["Incidence"].append(country.covid_incidence)
        final_csv["Quality of Life"].append(country.quality_of_life)
        final_csv["Health Care"].append(country.health_care.index)
        final_csv["Skill"].append(country.health_care.skill)
        final_csv["Speed"].append(country.health_care.speed)
        final_csv["Equipment"].append(country.health_care.equipment)
        final_csv["Accuracy"].append(country.health_care.accuracy)
        final_csv["Friendliness"].append(country.health_care.friendliness)
        final_csv["Responsiveness"].append(country.health_care.responsiveness)
        final_csv["Cost"].append(country.health_care.cost)
        final_csv["Location"].append(country.health_care.location)
        final_csv["Traffic"].append(country.traffic.index)
        final_csv["Time"].append(country.traffic.time)
        final_csv["Inefficiency"].append(country.traffic.inefficiency)
        final_csv["Pollution"].append(country.pollution)
        final_csv["Climate"].append(country.climate)
        for city in country.cities:
            final_csv["Name"].append(city.name)
            final_csv["Fatality"].append(city.covid_fatality)
            final_csv["Incidence"].append(city.covid_incidence)
            final_csv["Quality of Life"].append(city.quality_of_life)
            final_csv["Health Care"].append(city.health_care.index)
            final_csv["Skill"].append(city.health_care.skill)
            final_csv["Speed"].append(city.health_care.speed)
            final_csv["Equipment"].append(city.health_care.equipment)
            final_csv["Accuracy"].append(city.health_care.accuracy)
            final_csv["Friendliness"].append(city.health_care.friendliness)
            final_csv["Responsiveness"].append(city.health_care.responsiveness)
            final_csv["Cost"].append(city.health_care.cost)
            final_csv["Location"].append(city.health_care.location)
            final_csv["Traffic"].append(city.traffic.index)
            final_csv["Time"].append(city.traffic.time)
            final_csv["Inefficiency"].append(city.traffic.inefficiency)
            final_csv["Pollution"].append(city.pollution)
            final_csv["Climate"].append(city.climate)
    final = pd.DataFrame.from_dict(final_csv)
    df = final.loc[(final["Fatality"] != 0.0) & (final["Incidence"] != 0.0)]
    df.to_csv("data_all.csv", index=False)
    pd.set_option('display.max_columns', None)
    print(df.corr()[["Fatality", "Incidence"]])
    # scraper.scrape_data()
    t2 = time.perf_counter() - t
    print(f"Total time taken : {t2:0.2f} seconds")
    scraper.save_data("zbibo.json")
