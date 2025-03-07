import mysql.connector
import nba_api

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Xy_300501@"
)

mycursor = mydb.cursor()

mycursor.execute("CREATE DATABASE NBA")
