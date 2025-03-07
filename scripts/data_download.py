from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import teamyearbyyearstats
from nba_api.stats.static import teams
from nba_api.stats.endpoints import teamdashboardbygeneralsplits

import mysql.connector
from mysql.connector import Error

def create_table(host, database, user, password, table_name, table):
    """
    general function to create mysql table

    Args:
        host (str): MySQL server host name
        database (str): MySQL database
        user (str): MySQL username
        password (str): MySQL password
        table_name (str): Name for table to be created
        table (pd.DataFrame): Dataframe with data to be put into the table

    Returns:
        None
    """
    try:
    # Connect to MySQL
        connection = mysql.connector.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        if connection.is_connected():
            cursor = connection.cursor()

            # Generate SQL CREATE TABLE statement
            create_table_statement = f"CREATE TABLE IF NOT EXISTS {table_name} ("
            for column in table.columns:
                dtype = table[column].dtype
                if dtype == 'int64':
                    sql_type = 'INT'
                elif dtype == 'float64':
                    sql_type = 'FLOAT'
                elif dtype == 'object':
                    sql_type = 'VARCHAR(255)'
                else:
                    sql_type = 'VARCHAR(255)'
                create_table_statement += f"{column} {sql_type}, "
            create_table_statement = create_table_statement.rstrip(', ') + ");"

            # Execute the CREATE TABLE statement
            cursor.execute(create_table_statement)
            print(f"Table '{table_name}' created successfully.")

            # Generate SQL INSERT INTO statement
            insert_statement = f"INSERT INTO {table_name} ({', '.join(table.columns)}) VALUES ({', '.join(['%s'] * len(table.columns))})"
            rows = [tuple(x) for x in table.to_numpy()]

            # Execute the INSERT INTO statement
            cursor.executemany(insert_statement, rows)
            connection.commit()
            print(f"Data inserted into '{table_name}' successfully.")

    except Error as e:
        print(f"Error: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed.")

def create_games_table(host, database, user, password):
    """
    Create table from leaguegamefinder data which is a history of played games

    Args:
        host (str): MySQL server host name
        database (str): MySQL database
        user (str): MySQL username
        password (str): MySQL password

    Returns:
        None
    """

    #restrict to regular season nba games
    games = leaguegamefinder.LeagueGameFinder(league_id_nullable="00", season_type_nullable= "Regular Season").get_data_frames()[0]
    
    table_name = "games_raw"
    create_table(host, database, user, password, table_name, games)

def create_team_stats(host, database, user, password):
    """
    Create table from teamyearbyyearstats data for each team, giving every teams season stats

    Args:
        host (str): MySQL server host name
        database (str): MySQL database
        user (str): MySQL username
        password (str): MySQL password

    Returns:
        None
    """
    team_list = teams.get_teams()

    for team in team_list:
        table_name = f"{team['abbreviation']}_stats_raw"
        team_stats = teamyearbyyearstats.TeamYearByYearStats(team_id=team['id']).get_data_frames()[0]

        create_table(host, database, user, password, table_name, team_stats)


if __name__ == "__main__":
    import json
    import os

    with open('config.json') as f:
        config = json.load(f)

    host = os.getenv('host', config['database']['host'])
    database = os.getenv('db', config['database']['database'])
    user = os.getenv('user', config['database']['user'])
    password = os.getenv('password', config['database']['password'])

    create_games_table(host, database, user, password)
    create_team_stats(host, database, user, password)

