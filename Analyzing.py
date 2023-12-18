import pandas as pd
from AnalysisFunctions import *

if __name__ == '__main__':
    df, table_name = read_table()

    pd.set_option('display.max_columns', None)

    match table_name:
        case 'covid_country_daily':
            questions = [
                inquirer.List('Plot',
                              message="choose an option:",
                              choices=["1: one country's data", "2: two countries comparison"],
                              ),
            ]
            answer = inquirer.prompt(questions)
            if answer['Plot'] == "1: one country's data":
                country_list()
                country = country()
                data = df[df['location'] == country]
                columns = list(data.columns)
                del columns[0]
                plotting_2d(data, columns)
            if answer['Plot'] == "2: two countries comparison":
                plotting_countries_comparison(table_name)

        case 'test_vaccination_country_daily':
            questions = [
                inquirer.List('Plot',
                              message="choose an option:",
                              choices=["1: one country's data", "2: two countries comparison"],
                              ),
            ]
            answer = inquirer.prompt(questions)
            if answer['Plot'] == "1: one country's data":
                country_list()
                country = country()
                data = df[df['location'] == country]
                print("For this dataset X-Axes Should always be date")
                columns = list(data.columns)
                del columns[0]
                plotting_2d(data, columns)
            if answer['Plot'] == "2: two countries comparison":
                plotting_countries_comparison(table_name)

        case 'covid_country':
            plotting_all_countries_comparison(table_name)
        case 'test_vaccination_country':
            plotting_all_countries_comparison(table_name)
        case 'world_covid_daily':
            x_axis = df['date']
            y1_axis = df['world_cases']
            y2_axis = df['world_deaths']
            fig, [ax1, ax2] = plt.subplots(nrows=2, ncols=1, sharex=True)
            ax1.scatter(x_axis, y1_axis, alpha=0.5)
            ax1.set_title("date vs world_cases")
            ax2.scatter(x_axis, y2_axis, alpha=0.5)
            ax2.set_title("date vs world_deaths")
            plt.show()
        case 'world_test_vaccination_daily':
            world_test_vaccination_daily(df)
        case 'world_covid_monthly':
            world_covid_monthly(table_name)
        case 'world_test_vaccination_monthly':
            world_test_vaccination_monthly(table_name)
        case 'comparison_in_a_country':
            plotting_comparison()
