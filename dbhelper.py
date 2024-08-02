import pyodbc as pyodbc
import logging

class DB:

    def __init__(self):
        logger = logging.getLogger(__name__)
        #connect to the AWS MSSQL database - portfolio.cpououw2sybk.us-east-2.rds.amazonaws.com
        try:
            server='oltp.cpououw2sybk.us-east-2.rds.amazonaws.com'
            username='admin'
            password='Admin1234'
            database='Flights'
            driver ='{ODBC Driver 17 for SQL Server}'
            connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'

            # Establish a connection
            connection = pyodbc.connect(connection_string)
            print("Connection successful!")

            # Create a cursor from the connection
            self.mycursor = connection.cursor()
        except:
            logger.error(f"Database connection error: {e}")

    def fetch_city_names(self):

        city = []
        self.mycursor.execute("""
        SELECT DISTINCT(Dep_CityName) FROM USFlightsJan1Wk
        UNION
        SELECT DISTINCT(Arr_CityName) FROM USFlightsJan1Wk
        """)

        data = self.mycursor.fetchall()

        for item in data:
            city.append(item[0])

        return city

    def fetch_all_flights(self, source, destination):
        self.mycursor.execute("""
        SELECT Airline,DepTime_label,Flight_Duration,
        Distance_type FROM USFlightsJan1Wk
        WHERE Dep_CityName = '{}' AND Arr_cityName = '{}'
        """.format(source, destination))

        data = self.mycursor.fetchall()

        return data

    def fetch_airline_frequency(self):

        airline = []
        frequency = []

        self.mycursor.execute("""
        SELECT Airline,COUNT(*) FROM USFlightsJan1Wk
        GROUP BY Airline
        """)

        data = self.mycursor.fetchall()

        for item in data:
            airline.append(item[0])
            frequency.append(item[1])

        return airline, frequency

    def busy_airport(self):

        city = []
        frequency = []

        self.mycursor.execute("""
        SELECT Dep_CityName,COUNT(*) FROM (SELECT Dep_CityName FROM USFlightsJan1Wk
        UNION ALL
        SELECT Arr_CityName FROM USFlightsJan1Wk) t
        GROUP BY t.Dep_CityName
        ORDER BY COUNT(*) DESC
        """)

        data = self.mycursor.fetchall()

        for item in data:
            city.append(item[0])
            frequency.append(item[1])

        return city, frequency

    def daily_frequency(self):

        date = []
        frequency = []

        self.mycursor.execute("""
        SELECT FlightDate,COUNT(*) FROM USFlightsJan1Wk
        GROUP BY FlightDate
        """)

        data = self.mycursor.fetchall()

        for item in data:
            date.append(item[0])
            frequency.append(item[1])

        return date, frequency






