import json
import sqlalchemy

def date_str(df_date):
    return str(int(df_date['year'])) + '-' + str(int(df_date['month'])) + '-' + str(int(df_date['day']))

def add_flight_booking(request):
    
    request_json = request.get_json()

    passenger_name = request_json['sessionInfo']['parameters']['passenger_name']
    departure_city = request_json['sessionInfo']['parameters']['departure_city']
    departure_date = request_json['sessionInfo']['parameters']['departure_date']
    destination_city = request_json['sessionInfo']['parameters']['destination_city']
    return_date = request_json['sessionInfo']['parameters']['return_date']
    
    print(f'CF DEBUG: passenger_name:{passenger_name}')
    print(f'CF DEBUG: departure_city:{departure_city}')
    print(f'CF DEBUG: departure_date:{departure_date}')
    print(f'CF DEBUG: destination_city:{destination_city}')
    print(f'CF DEBUG: return_date:{return_date}')

    db_user = 'postgres'
    db_pass = 'Fl1ghtBook1ng'
    db_name = 'postgres'
    db_socket_dir = '/cloudsql'
    cloud_sql_connection_name = 'hwk-ccai-appmod-demo:australia-southeast1:flight-booking'
    
    db = sqlalchemy.create_engine(
        sqlalchemy.engine.url.URL.create(
            drivername='postgresql+pg8000',
            username=db_user,
            password=db_pass,
            database=db_name,
            query={
                'unix_sock': '{}/{}/.s.PGSQL.5432'.format(
                    db_socket_dir,
                    cloud_sql_connection_name)
            }
        ),
    )
    
    print("CF DEBUG: Executing SQL")
    sql_stmt = ("INSERT INTO flight_booking (passenger_name, departure_city, departure_date, destination_city, return_date)" 
                "VALUES ('{}','{}','{}','{}','{}')")


    try:
        with db.connect() as conn:
            conn.execute(sql_stmt.format(passenger_name, departure_city, date_str(departure_date), destination_city, date_str(return_date)))
    except Exception as e:
        print(f'CF Exception: SQL Execution Error:{str(e)}')
        return 'webhook error'
    

    print("CF DEBUG: Executed SQL")

    response_json = json.dumps({
        'sessionInfo': {
            'parameters': {
                'flightBooked': True
            }
        }
    })
    return response_json
