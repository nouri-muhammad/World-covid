import pandas as pd

df = pd.read_csv('world-covid-data.csv', parse_dates=['date'])

# Keeping useful columns
columns = ['iso_code', 'continent', 'location', 'date', 'total_cases', 'total_deaths', 'reproduction_rate',
           'icu_patients', 'hosp_patients', 'total_tests', 'positive_rate', 'total_vaccinations', 'people_vaccinated',
           'people_fully_vaccinated', 'total_boosters', 'gdp_per_capita', 'extreme_poverty', 'cardiovasc_death_rate',
           'diabetes_prevalence', 'female_smokers', 'male_smokers', 'hospital_beds_per_thousand', 'life_expectancy',
           'human_development_index', 'population']
df = df[columns]

# list of countries
countries = list(df['location'].unique())

# delete data of continents (These data bring inconsistency).
ind = df[df['continent'].isnull()].index
df.drop(ind, inplace=True)

# delete data of countries in which total_cases, total_death, total_tests and total_vaccinations columns are all NULL
# data of 177 countries from 255 remains
for country in countries:
    data = df[df['location'] == country]
    if (data['total_cases'].sum() == 0 or data['total_deaths'].sum() == 0 or data['total_tests'].sum() == 0 or
            data['total_vaccinations'].sum() == 0):
        ind = df[df['location'] == country].index
        df.drop(ind, inplace=True)

# split the data into two different data sets   1. covid data set   2. test-vaccination data set
covid_columns = ['iso_code', 'continent', 'location', 'date', 'total_cases', 'total_deaths', 'reproduction_rate',
                 'icu_patients', 'hosp_patients', 'gdp_per_capita', 'extreme_poverty', 'cardiovasc_death_rate',
                 'diabetes_prevalence', 'female_smokers', 'male_smokers', 'hospital_beds_per_thousand',
                 'life_expectancy', 'human_development_index', 'population']
test_vaccination_columns = ['iso_code', 'continent', 'location', 'date', 'total_tests', 'positive_rate',
                            'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated', 'total_boosters',
                            'population']

df[covid_columns].to_csv(path_or_buf="/tmp/CovidDeath.csv'", sep=',', index=False, encoding='utf-8')
df[test_vaccination_columns].to_csv(path_or_buf="/tmp/CovidVaccination.csv", sep=',', index=False, encoding='utf-8')
