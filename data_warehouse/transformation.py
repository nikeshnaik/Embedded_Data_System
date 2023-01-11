from utility.helpers import DuckDBConn, SQLiteConn


def transform(dwh_path, data_lake_file):

    with DuckDBConn(dwh_path) as cursor:
        result = cursor.execute("CREATE OR REPLACE TABLE user_behaviour_metric(customer_id integer, amount_spent double, review_score integer, review_count integer, last_update date);")

    with SQLiteConn(data_lake_file) as data_lake_cursor:


        #Todo: Write join query in sqlite and dump to duckdb.
        data_lake_cursor.execute("""select
                            cr.customer_id as customer_id,
                            Round(SUM(CAST(up.quantity AS INTEGER) * CAST(REPLACE(up.unite_price, "$", "") AS DOUBLE) ),
                            5) as amount_spent,
                            sum(cr.is_positive) as review_score,
                            count(cr.customer_id) as review_count
                        from
                            USER_PURCHASE up
                        join CUSTOMER_REVIEWS cr on
                            cr.customer_id = up.customer_id
                        group by
                            up.customer_id;  
                        """)
        
        with DuckDBConn(dwh_path) as warehouse_cursor:


            warehouse_cursor.execute("DELETE FROM user_behaviour_metric;")

            while record:=data_lake_cursor.fetchone():

                insert_query = f""" 

                        INSERT INTO user_behaviour_metric(customer_id, amount_spent, review_score, review_count, last_update) VALUES ({record[0]}, {record[1]}, {record[2]},{record[3]}, today());
                
                """
                print(insert_query) 
                warehouse_cursor.execute(insert_query)




    print("Transformation Done and dumped into Warehouse")

if __name__ == "__main__":

    data_warehouse_file = "data_warehouse/memory_warehouse.duckdb"
    data_lake_file = "data_lake/stage/intermediate_state.db"

    transform(data_warehouse_file, data_lake_file)


    


    