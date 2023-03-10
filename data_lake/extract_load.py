import sqlite3
import json
from utility.helpers import SQLiteConn, movie_review_generator

def convertingJsonToSQLTable(customer_reviews_file, intermediate_state):


    review_record = movie_review_generator(customer_reviews_file)

    try:
        with SQLiteConn(intermediate_state) as cursor:

            while record:=next(review_record):

                record["is_positive"] = 1 if record["is_positive"] else 0

                insert_query = f"""INSERT INTO CUSTOMER_REVIEWS (customer_id, is_positive) VALUES (?,?);"""

                cursor.execute(insert_query, (record["customer_id"], record["is_positive"]))
            

    except StopIteration as e:
        print("customer reviews generator stop iteration")

    except Exception as e:
        print("Exeception Occured:", str(e))


def createIntermediateTable(intermediate_state):

    with SQLiteConn(intermediate_state) as cursor:

        cursor.execute(f"DROP TABLE IF EXISTS CUSTOMER_REVIEWS;")

        cursor.execute(f"CREATE TABLE IF NOT EXISTS CUSTOMER_REVIEWS (customer_id INTEGER, is_positive INTEGER);")



def mergeUserDataWithIntermediate(intermediate_state, user_data):

    with SQLiteConn(intermediate_state) as intermediate_state_cursor:

        intermediate_state_cursor.execute("DROP TABLE IF EXISTS USER_PURCHASE;")

        ## Todo: Create user purchase table in intermediate table, insert all records below
        intermediate_state_cursor.execute("CREATE TABLE IF NOT EXISTS USER_PURCHASE (invoice_number TEXT, stock_code TEXT, details TEXT, quantity INTEGER, invoice_date DATE, unite_price TEXT, customer_id INTEGER, zipcode TEXT )")

        with SQLiteConn(user_data) as user_data_cursor:

            user_data_cursor.execute("select * from user_purchase;")

            while record:=user_data_cursor.fetchone():
                print(record)
                insert_query = f"""
                        INSERT INTO USER_PURCHASE (invoice_number, stock_code, details, quantity,invoice_date, unite_price, customer_id, zipcode) VALUES("{record[0]}","{record[1]}","{record[2]}","{record[3]}","{record[4]}","{record[5]}","{record[6]}","{record[7]}")
                """
                print(insert_query)
                intermediate_state_cursor.execute(insert_query)
            


def extract_load_task(customer_reviews_file, user_data, intermediate_state):
    createIntermediateTable(intermediate_state)
    convertingJsonToSQLTable(customer_reviews_file, intermediate_state)
    mergeUserDataWithIntermediate(intermediate_state, user_data) 

if __name__ == "__main__":

    customer_reviews_file = "data_lake/raw/Classified_movie_review.json"
    user_data = "data_lake/raw/user_purchase.sqlite"
    intermediate_state = "data_lake/stage/intermediate_state.db"

    # # createIntermediateTable(intermediate_state)

    # # convertingJsonToSQLTable(customer_reviews_file, intermediate_state)

    # mergeUserDataWithIntermediate(intermediate_state, user_data)

    





 



    

