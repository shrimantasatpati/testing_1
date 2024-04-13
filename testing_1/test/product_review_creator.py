import sqlite3

# Create a new SQLite3 database file
conn = sqlite3.connect('product_reviews.db')

# Get a cursor object
cursor = conn.cursor()

# Create a table
cursor.execute('''CREATE TABLE reviews
                  (id INTEGER PRIMARY KEY,
                   company_name TEXT,
                   product_name TEXT,
                   review_text TEXT,
                   rating INTEGER)''')

# Insert some sample data
cursor.execute("INSERT INTO reviews (company_name, product_name, review_text, rating) VALUES ('Apple', 'iPhone 13', 'Great camera and performance, but battery life could be better.', 4)")
cursor.execute("INSERT INTO reviews (company_name, product_name, review_text, rating) VALUES ('Samsung', 'Galaxy S22', 'Excellent display and design, but the price is a bit high.', 5)")
cursor.execute("INSERT INTO reviews (company_name, product_name, review_text, rating) VALUES ('Sony', 'WH-1000XM4 Headphones', 'Superb noise-cancelling and sound quality, but a bit bulky.', 4)")

# Commit the changes
conn.commit()

# Close the connection
conn.close()