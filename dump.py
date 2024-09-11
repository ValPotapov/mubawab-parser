import json

import mysql.connector


connection = mysql.connector.connect(
    host="localhost",
    database="adv",
    user="root",
    password="root"
)

cursor = connection.cursor()

with open("data/new_advs.json", mode="r", encoding="utf-8") as json_file:
    data = json.loads(json_file.read())

for entry in data:
    if isinstance(entry.get("phone_numbers"), list):
        entry["phone_numbers"] = json.dumps(entry["phone_numbers"])
    if isinstance(entry.get("tags"), list):
        entry["tags"] = json.dumps(entry["tags"])
    if isinstance(entry.get("ad_features"), list):
        entry["ad_features"] = json.dumps(entry["ad_features"])
    if isinstance(entry.get("location"), list):
        entry["location"] = json.dumps(entry["location"])
    if isinstance(entry.get("photos_urls"), list):
        entry["photos_urls"] = json.dumps(entry["photos_urls"])

    cursor.execute("""
        INSERT INTO advertisements (
            property_type, is_new, url, title, description, area, floor, rooms_number, 
            price, price_per_day, from_price, rent_price, request_price, currency, district, 
            region, publish_date, phone_numbers, tags, ad_features, elevator, 
            location, photos_urls, age, relevant
        ) 
        VALUES (
            %(property_type)s, %(new)s, %(url)s, %(title)s, %(description)s, %(area)s, %(floor)s, %(rooms_number)s, 
            %(price)s, %(price_per_day)s, %(from_price)s, %(rent_price)s, %(request_price)s, %(currency)s, %(district)s, 
            %(region)s, %(publish_date)s, %(phone_numbers)s, %(tags)s, %(ad_features)s, %(elevator)s, 
            %(location)s, %(photos_urls)s, %(age)s, %(relevant)s
        )
    """, entry)

connection.commit()

cursor.close()
connection.close()
