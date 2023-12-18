import psycopg2 as pg
from AnalysisFunctions import connection_info

# creating connection
conn = pg.connect(**connection_info())
cur = conn.cursor()

# create different tables to get insight from

# covid_death based on (country) and (country, date)
cur.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS covid_country_daily\
            AS\
                SELECT \
                    location,\
                    date,\
                    max(population) AS population,\
                    max(total_cases) AS total_case,\
                    max(total_deaths) AS total_death,\
                    ROUND(max(total_deaths) / max(total_cases) * 100, 4) AS death_per_100_case,\
                    ROUND(max(total_cases) / max(population) * 100, 4) AS case_in_population,\
                    ROUND(max(total_deaths) / max(population) * 100, 4) AS death_in_population\
                FROM covid_death\
                GROUP BY GROUPING SETS ((location), (location, date))\
                ORDER BY location, date;")
conn.commit()

# test & vaccination based on (country) and (country, date)
cur.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS test_vaccination_country_daily\
            AS\
                SELECT \
                    location,\
                    date,\
                    MAX(total_tests) AS total_tests,\
                    ROUND(AVG(positive_rate), 4) AS positive_rate,\
                    MAX(total_vaccinations) AS total_vaccinations,\
                    MAX(people_vaccinated) AS people_vaccinated,\
                    MAX(people_fully_vaccinated) AS people_fully_vaccinated,\
                    MAX(population) AS population\
                FROM covid_test_vaccination\
                GROUP BY GROUPING SETS ((location), (location, date))\
                ORDER BY location, date;")
conn.commit()

# covid based on country
cur.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS covid_country\
            AS\
                SELECT \
                    location,\
                    MAX(population) AS population,\
                    MAX(total_cases) AS total_case,\
                    MAX(total_deaths) AS total_death,\
                    ROUND(MAX(total_deaths) / MAX(total_cases) * 100, 4) AS death_per_100_case,\
                    ROUND(MAX(total_cases) / MAX(population) * 100, 4) AS case_in_population,\
                    ROUND(MAX(total_deaths) / MAX(population) * 100, 4) AS death_in_population,\
                    MAX(gdp_per_capita) AS gdp_per_capita,\
                    MAX(extreme_poverty) AS extreme_poverty,\
                    MAX(cardiovasc_death_rate) AS cardiovasc_death_rate,\
                    MAX(diabetes_prevalence) AS diabetes_prevalence,\
                    MAX(female_smokers) AS female_smokers,\
                    MAX(male_smokers) AS male_smokers,\
                    MAX(life_expectancy) AS life_expectancy\
                FROM covid_death\
                GROUP BY location\
                ORDER BY location;")
conn.commit()

# test & vaccination based on country
cur.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS test_vaccination_country\
            AS\
                SELECT \
                    location,\
                    MAX(total_tests) AS total_tests,\
                    ROUND(AVG(positive_rate), 4) AS positive_rate,\
                    MAX(total_vaccinations) AS total_vaccinations,\
                    MAX(people_vaccinated) AS people_vaccinated,\
                    MAX(people_fully_vaccinated) AS people_fully_vaccinated,\
                    max(population) AS population\
                FROM covid_test_vaccination\
                GROUP BY location\
                ORDER BY location;")
conn.commit()

# covid based on date (world covid daily)
cur.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS world_covid_daily\
            AS\
                SELECT \
                    date,\
                    SUM(total_cases) AS world_cases,\
                    SUM(total_deaths) AS world_deaths\
                FROM covid_death\
                GROUP BY date\
                ORDER BY date;")
conn.commit()

# test & vaccination on date (world test & vaccination daily)
cur.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS world_test_vaccination_daily\
            AS\
                SELECT\
                    date,\
                    SUM(total_tests) AS total_tests,\
                    ROUND(AVG(positive_rate), 4) AS positive_rate,\
                    SUM(total_vaccinations) AS total_vaccinations,\
                    SUM(people_vaccinated) AS people_vaccinated,\
                    SUM(people_fully_vaccinated) AS people_fully_vaccinated\
                FROM\
                (\
                    SELECT \
                        location,\
                        date,\
                        MAX(total_tests) AS total_tests,\
                        ROUND(AVG(positive_rate), 4) AS positive_rate,\
                        MAX(total_vaccinations) AS total_vaccinations,\
                        MAX(people_vaccinated) AS people_vaccinated,\
                        MAX(people_fully_vaccinated) AS people_fully_vaccinated\
                    FROM covid_test_vaccination\
                    GROUP BY location, date\
                    ORDER BY location, date\
                )\
                GROUP BY date\
                ORDER BY date;")
conn.commit()

# covid based on month (world covid monthly)
cur.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS world_covid_monthly\
            AS\
                SELECT\
                    years,\
                    months,\
                    SUM(total_cases) AS total_cases,\
                    SUM(world_deaths) AS world_deaths,\
                    ROUND(AVG(world_death_per_100_case), 2) AS world_death_per_100_case\
                FROM\
                (\
                    SELECT\
                            location,\
                            to_char(date, 'YYYY') AS years,\
                            EXTRACT(month FROM date) AS months,\
                            max(total_cases) AS total_cases,\
                            max(total_deaths) AS world_deaths,\
                            round(sum(total_deaths) / sum(total_cases) * 100, 4) AS world_death_per_100_case\
                    FROM covid_death\
                    GROUP BY location, (to_char(date, 'YYYY')), (EXTRACT(month FROM date))\
                    ORDER BY location, years, months\
                )\
                GROUP BY years, months\
                ORDER BY years, months;")
conn.commit()

# test & vaccination on month (world test & vaccination monthly)
cur.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS world_test_vaccination_monthly\
            AS\
                SELECT\
                    years,\
                    months,\
                    SUM(total_tests),\
                    round(avg(positive_rate), 4) AS positive_rate,\
                    SUM(total_vaccinations) AS total_vaccinations,\
                    SUM(people_vaccinated) AS people_vaccinated,\
                    SUM(people_fully_vaccinated) AS people_fully_vaccinated\
                FROM\
                (\
                    SELECT \
                        location,\
                        to_char(date::timestamp with time zone, 'YYYY'::text) AS years,\
                        EXTRACT(month FROM date) AS months,\
                        MAX(total_tests) AS total_tests,\
                        round(avg(positive_rate), 4) AS positive_rate,\
                        MAX(total_vaccinations) AS total_vaccinations,\
                        MAX(people_vaccinated) AS people_vaccinated,\
                        MAX(people_fully_vaccinated) AS people_fully_vaccinated\
                    FROM covid_test_vaccination\
                    GROUP BY location, (to_char(date, 'YYYY')), (EXTRACT(month FROM date))\
                    ORDER BY location, years, months\
                )\
                GROUP BY years, months\
                ORDER BY years, months;")
conn.commit()


cur.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS world_summary\
            AS\
                SELECT\
                    SUM(total_case) AS total_case,\
                    SUM(total_death) AS total_death,\
                    ROUND(SUM(total_death)::Numeric/SUM(total_case)*100, 4)  AS death_per_case\
                FROM covid_country;")
conn.commit()

cur.close()
conn.close()
