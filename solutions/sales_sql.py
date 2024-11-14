import sqlite3
import csv

conn = sqlite3.connect('S30_db.db')
cur = conn.cursor()

query = """
    SELECT 
    c.customer_id AS Customer,
    c.age AS Age,
    i.item_name AS Item,
    CAST(SUM(COALESCE(o.quantity, 0)) AS INTEGER) AS Quantity
FROM 
    sales s
JOIN 
    customers c ON s.customer_id = c.customer_id
JOIN 
    orders o ON s.sales_id = o.sales_id
JOIN 
    items i ON o.item_id = i.item_id
WHERE 
    c.age BETWEEN 18 AND 35
GROUP BY 
    c.customer_id, c.age, i.item_name
HAVING 
    Quantity > 0

"""
filtered_result = cur.execute(query).fetchall()

with open('output/filtered_data_sql.csv', mode='w', newline='') as file:
    writer = csv.writer(file, delimiter=';')
    writer.writerow(['Customer', 'Age', 'Item', 'Quantity'])  
    writer.writerows(filtered_result)

print("Data saved to csv file..")

cur.close()
conn.close()
