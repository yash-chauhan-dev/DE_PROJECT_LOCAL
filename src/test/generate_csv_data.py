import csv
from resources.dev import config
import os
import random
from datetime import datetime, timedelta

customer_ids = list(range(1, 51))
store_ids = list(range(121, 124))
product_data = {
    "quaker oats": 212,
    "sugar": 50,
    "maida": 20,
    "besan": 52,
    "refined oil": 110,
    "clinic plus": 1.5,
    "dantkanti": 100,
    "nutrella": 40
}
sales_persons = {
    121: [id for id in range(1, 16)],
    122: [id for id in range(16, 31)],
    123: [id for id in range(31, 51)]
}

start_date = datetime(2024, 3, 3)
end_date = datetime(2024, 8, 20)

file_location = config.local_data_dump
csv_file_path = os.path.join(file_location, "sales_data.csv")
with open(csv_file_path, "w", newline="") as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["customer_id", "store_id", "product_name",
                       "sales_date", "sales_person_id", "price", "quantity", "total_cost"])

    for _ in range(5000):
        customer_id = random.choice(customer_ids)
        store_id = random.choice(store_ids)
        product_name = random.choice(list(product_data.keys()))
        sales_date = start_date + \
            timedelta(days=random.randint(0, (end_date - start_date).days))
        sales_person_id = random.choice(sales_persons[store_id])
        quantity = random.randint(1, 10)
        price = product_data[product_name]
        total_cost = price * quantity

        csvwriter.writerow([customer_id, store_id, product_name, sales_date.strftime(
            "%Y-%m-%d"), sales_person_id, price, quantity, total_cost])

print("CSV file generated successfully.")
