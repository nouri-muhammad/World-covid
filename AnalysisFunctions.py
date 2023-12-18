import maskpass as mp
import pandas.io.sql as sqlio
import psycopg2 as pg
import matplotlib.pyplot as plt
import seaborn as sb
import inquirer
from matplotlib.font_manager import FontProperties
sb.set_style("whitegrid", {'axes.grid': False})


def connection_info():
    host = mp.askpass(prompt="enter the host ip: ", mask="")
    dbname = mp.askpass(prompt="enter database name: ", mask="")
    user = mp.askpass(prompt="enter user name: ", mask="")
    pwd = mp.askpass(prompt="enter password: ", mask="")
    pg_connection_dict = {
        'host': host,
        'dbname': dbname,
        'user': user,
        'password': pwd
    }
    return pg_connection_dict


def read_tables(table):
    sql = f"SELECT * FROM {table}"
    conn = pg.connect(**connection_info())
    cur = conn.cursor()
    dat = sqlio.read_sql_query(sql, conn)
    cur.close()
    conn.close()
    return dat


def country():
    """
    This function lets user choose a country for further analysis.
    """
    while True:
        try:
            country_name = str(input("Country: "))
            return country_name
        except ValueError:
            print("Invalid input ")


def country_list():
    """
    This function provides the user a list of all countries.
    """
    questions = [
        inquirer.List('List',
                      message="Do you need the list of countries?",
                      choices=['Y', 'N'],
                      ),
    ]
    answer = inquirer.prompt(questions)
    if answer['List'] == 'Y':
        df = read_tables('covid_country')
        countries = df['location'].unique()
        print(countries)


def plotting_2d(dataframe, columns: list):
    """
    This function plots a 2-D scatter plot for user.
    """
    question_x = [
        inquirer.List('x_axes',
                      message="Choose X axes?",
                      choices=columns,
                      ),
    ]
    question_y = [
        inquirer.List('y_axes',
                      message="Choose Y axes?",
                      choices=columns,
                      ),
    ]
    x_axes = inquirer.prompt(question_x)
    columns.remove(x_axes['x_axes'])
    y_axes = inquirer.prompt(question_y)
    sb.relplot(data=dataframe, x=dataframe['date'], y=y_axes['y_axes'], kind='scatter')
    sb.set_style('darkgrid')
    plt.xticks(rotation=45)
    plt.show()


def plotting_distribution(dataframe, columns: list):
    """
    This function plots a histogram distribution for user.
    """
    question_x = [
        inquirer.List('column',
                      message="Which data do you want?",
                      choices=columns,
                      ),
    ]
    distribution = inquirer.prompt(question_x)
    plt.hist(dataframe[distribution['column']], bins=50, density=True, histtype="step")
    plt.show()


def world_test_vaccination_daily(dataframe):
    x_axis = dataframe['date']
    fig, [[ax1, ax2], [ax3, ax4], [ax5, ax6]] = plt.subplots(nrows=3, ncols=2)
    ax1.plot(x_axis, dataframe['total_tests'])
    ax1.set(ylabel='total_tests')
    ax1.grid()
    ax2.plot(x_axis, dataframe['positive_rate'])
    ax2.set(ylabel='positive_rate')
    ax2.grid()
    ax3.plot(x_axis, dataframe['total_vaccinations'])
    ax3.set(ylabel='total_vaccinations')
    ax3.grid()
    ax4.plot(x_axis, dataframe['people_vaccinated'])
    ax4.set(ylabel='people_vaccinated')
    ax4.grid()
    ax5.plot(x_axis, dataframe['people_fully_vaccinated'])
    ax5.set(ylabel='people_fully_vaccinated')
    ax5.grid()
    plt.show()


def world_test_vaccination_monthly(name):
    sql = (f"SELECT\
    CONCAT(years, months) AS date , total_tests, positive_rate, total_vaccinations, people_vaccinated,\
    people_fully_vaccinated\
    FROM {name}")
    conn = pg.connect(**connection_info())
    cur = conn.cursor()
    dataframe = sqlio.read_sql_query(sql, conn)
    cur.close()
    conn.close()

    x_axis = dataframe['date']
    fig, [[ax1, ax2], [ax3, ax4], [ax5, ax6]] = plt.subplots(nrows=3, ncols=2)
    ax1.plot(x_axis, dataframe['total_tests'])
    ax1.set_xticks(ax1.get_xticks()[::5])
    ax1.set(ylabel='total_tests')
    ax1.grid()
    ax2.plot(x_axis, dataframe['positive_rate'])
    ax2.set_xticks(ax2.get_xticks()[::5])
    ax2.set(ylabel='positive_rate')
    ax2.grid()
    ax3.plot(x_axis, dataframe['total_vaccinations'])
    ax3.set_xticks(ax3.get_xticks()[::5])
    ax3.set(ylabel='total_vaccinations')
    ax3.grid()
    ax4.plot(x_axis, dataframe['people_vaccinated'])
    ax4.set_xticks(ax4.get_xticks()[::5])
    ax4.set(ylabel='people_vaccinated')
    ax4.grid()
    ax5.plot(x_axis, dataframe['people_fully_vaccinated'])
    ax5.set_xticks(ax5.get_xticks()[::5])
    ax5.set(ylabel='people_fully_vaccinated')
    ax5.grid()
    plt.show()


def world_covid_monthly(name):
    sql = (f"SELECT\
    CONCAT(years, months) AS date , total_cases, world_deaths, world_death_per_100_case\
    FROM {name}")
    conn = pg.connect(**connection_info())
    cur = conn.cursor()
    dataframe = sqlio.read_sql_query(sql, conn)
    cur.close()
    conn.close()

    x_axis = dataframe['date']
    fig, [[ax1, ax2], [ax3, ax4]] = plt.subplots(nrows=2, ncols=2)
    ax1.plot(x_axis, dataframe['total_cases'])
    ax1.set_xticks(ax1.get_xticks()[::5])
    ax1.set(ylabel='total_cases')
    ax1.grid()
    ax2.plot(x_axis, dataframe['world_deaths'])
    ax2.set_xticks(ax2.get_xticks()[::5])
    ax2.set(ylabel='world_deaths')
    ax2.grid()
    ax3.plot(x_axis, dataframe['world_death_per_100_case'])
    ax3.set_xticks(ax3.get_xticks()[::5])
    ax3.set(ylabel='world_death_per_100_case')
    ax3.grid()
    plt.show()


def plotting_comparison():
    tables = ['covid_country_daily', 'test_vaccination_country_daily', 'covid_country', 'test_vaccination_country',
              'world_covid_daily', 'world_test_vaccination_daily', 'world_covid_monthly',
              'world_test_vaccination_monthly']

    questions = [
        inquirer.List('Table',
                      message="First Table?",
                      choices=tables,
                      ),
    ]
    table_name = inquirer.prompt(questions)
    df = read_tables(table_name['Table'])
    tables.remove(table_name['Table'])

    questions = [
        inquirer.List('Table',
                      message="Second Table?",
                      choices=tables,
                      ),
    ]
    table_name = inquirer.prompt(questions)
    data = read_tables(table_name['Table'])

    country_list()
    country_name = country()
    df = df[df['location'] == country_name]
    data = data[data['location'] == country_name]

    df_columns = list(df.columns)
    data_columns = list(data.columns)

    while True:
        print(f"data_columns: {data_columns}")
        questions = [
            inquirer.List('x_axis',
                          message="x_axis?",
                          choices=df_columns,
                          ),
        ]
        x_axis = inquirer.prompt(questions)
        if (x_axis['x_axis'] in df_columns) and (x_axis['x_axis'] in data_columns):
            break
        else:
            print("\n\n", "*"*80, "X-axis should be common in both tables", "*"*80, "\n\n")

    questions = [
        inquirer.List('df_y_axis',
                      message="y_axis for first table?",
                      choices=df_columns,
                      ),
    ]
    df_y_axis = inquirer.prompt(questions)

    questions = [
        inquirer.List('data_y_axis',
                      message="y_axis for second table?",
                      choices=data_columns,
                      ),
    ]
    data_y_axis = inquirer.prompt(questions)

    fig, [ax1, ax2] = plt.subplots(nrows=2, ncols=1, sharex=True)
    ax1.scatter(df[x_axis['x_axis']], df[df_y_axis['df_y_axis']], alpha=0.5)
    ax1.set_title(f"{x_axis['x_axis']} vs {df_y_axis['df_y_axis']}")
    ax2.scatter(data[x_axis['x_axis']], data[data_y_axis['data_y_axis']], alpha=0.5)
    ax2.set_title(f"{x_axis['x_axis']} vs {data_y_axis['data_y_axis']}")
    plt.show()


def plotting_countries_comparison(table_name):
    """
    Comparing the statistics of two different countries using barchart.
    """
    dataframe = read_tables(table_name)
    columns = list(dataframe.columns)
    country1 = country()
    country2 = country()
    dataframe = dataframe[(dataframe['location'] == country1) | (dataframe['location'] == country2)]
    question_x = [
        inquirer.List('x_axes',
                      message="Choose X axes?",
                      choices=columns,
                      ),
    ]
    x_axes = inquirer.prompt(question_x)
    columns.remove(x_axes['x_axes'])

    question_y = [
        inquirer.List('y_axes',
                      message="Choose Y axes?",
                      choices=columns,
                      ),
    ]
    y_axes = inquirer.prompt(question_y)

    sb.set_style('darkgrid')
    sb.scatterplot(
        data=dataframe,
        x=dataframe[x_axes['x_axes']],
        y=dataframe[y_axes['y_axes']],
        hue=dataframe['location']
    )
    plt.title(f"something")
    plt.xlabel(x_axes['x_axes'])
    plt.ylabel(y_axes['y_axes'])
    font_p = FontProperties()
    font_p.set_size('xx-small')
    plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0., ncol=5, prop=font_p, fontsize='xx-small')
    plt.show()


def plotting_all_countries_comparison(table_name):
    """
    Comparing the statistics of two different countries using barchart.
    """
    dataframe = read_tables(table_name)
    columns = list(dataframe.columns)
    question_x = [
        inquirer.List('x_axes',
                      message="Choose X axes?",
                      choices=columns,
                      ),
    ]
    x_axes = inquirer.prompt(question_x)
    columns.remove(x_axes['x_axes'])

    question_y = [
        inquirer.List('y_axes',
                      message="Choose Y axes?",
                      choices=columns,
                      ),
    ]
    y_axes = inquirer.prompt(question_y)

    sb.set_style('darkgrid')
    sb.scatterplot(
        data=dataframe,
        x=dataframe[x_axes['x_axes']],
        y=dataframe[y_axes['y_axes']],
        hue=dataframe['location']
    )
    plt.title(f"something")
    plt.xlabel(x_axes['x_axes'])
    plt.ylabel(y_axes['y_axes'])
    font_p = FontProperties()
    font_p.set_size('xx-small')
    plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0., ncol=5, prop=font_p)
    plt.show()


def read_table():
    """
    This function reads the specified table user have chosen.
    """
    tables = ['covid_country_daily', 'test_vaccination_country_daily', 'covid_country', 'test_vaccination_country',
              'world_covid_daily', 'world_test_vaccination_daily', 'world_covid_monthly',
              'world_test_vaccination_monthly', 'comparison_in_a_country']
    questions = [
        inquirer.List('Table',
                      message="First Table?",
                      choices=tables,
                      ),
    ]
    table_name = inquirer.prompt(questions)
    if table_name['Table'] != 'comparison_in_a_country':
        df = read_tables(table_name['Table'])
        tables.remove(table_name['Table'])
        return df, table_name['Table']
    else:
        return None, 'comparison_in_a_country'
