import psycopg2 as pg
from AnalysisFunctions import connection_info

# creating connection
conn = pg.connect(**connection_info())
cur = conn.cursor()

# creating covid death table in project database
cur.execute("CREATE TABLE IF NOT EXISTS covid_death\
            (iso_code VARCHAR(10),\
            continent VARCHAR(13),\
            location VARCHAR(50),\
            date DATE,\
            total_cases NUMERIC,\
            total_deaths NUMERIC,\
            reproduction_rate NUMERIC,\
            icu_patients NUMERIC,\
            hosp_patients NUMERIC,\
            gdp_per_capita NUMERIC,\
            extreme_poverty NUMERIC,\
            cardiovasc_death_rate NUMERIC,\
            diabetes_prevalence NUMERIC,\
            female_smokers NUMERIC,\
            male_smokers NUMERIC,\
            hospital_beds_per_thousand NUMERIC,\
            life_expectancy NUMERIC,\
            human_development_index NUMERIC,\
            population NUMERIC);")
conn.commit()

# inserting data in covid death table from csv
cur.execute("COPY covid_death (iso_code, continent, location, date, total_cases, total_deaths,reproduction_rate,\
                                icu_patients, hosp_patients, gdp_per_capita, extreme_poverty, cardiovasc_death_rate,\
                                diabetes_prevalence, female_smokers, male_smokers, hospital_beds_per_thousand,\
                                life_expectancy, human_development_index, population)\
            FROM '/tmp/CovidDeath.csv'\
            DELIMITER ','\
            CSV HEADER;")
conn.commit()

# creating covid test vaccination table in portfolioProject database
cur.execute("CREATE TABLE IF NOT EXISTS covid_test_vaccination\
            (iso_code VARCHAR(10),\
            continent VARCHAR(13),\
            location VARCHAR(50),\
            date DATE,\
            total_tests NUMERIC,\
            positive_rate NUMERIC,\
            total_vaccinations NUMERIC,\
            people_vaccinated NUMERIC,\
            people_fully_vaccinated NUMERIC,\
            total_boosters NUMERIC,\
            population NUMERIC);")
conn.commit()

# inserting data in covid test vaccination table from csv
cur.execute("COPY covid_test_vaccination (iso_code, continent, location, date, total_tests, positive_rate,\
                                        total_vaccinations, people_vaccinated, people_fully_vaccinated, total_boosters,\
                                        population)\
            FROM '/tmp/CovidVaccination.csv'\
            DELIMITER ','\
            CSV HEADER;")
conn.commit()

cur.close()
conn.close()
