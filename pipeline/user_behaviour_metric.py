import schedule
from data_lake.extract_load import extract_load_task
from data_warehouse.transformation import transform
import time


def job():

    data_warehouse_file = "data_warehouse/memory_warehouse.duckdb"
    data_lake_file = "data_lake/stage/intermediate_state.db"

    customer_reviews_file = "data_lake/raw/Classified_movie_review.json"
    user_data = "data_lake/raw/user_purchase.sqlite"

    extract_load_task(customer_reviews_file, user_data, data_lake_file)
    transform(data_warehouse_file, data_lake_file)


if __name__ == "__main__":
    schedule.every().day.at("16:28").do(job)
    print("Start executing jobs at predefined time...")
    print(schedule.get_jobs())
 

    while True:
        schedule.run_pending()
        time.sleep(1)

    print("All jobs executed.")

