import sqlite3

# connect to the SQLite database
connection = sqlite3.connect('air_bnb_database.db')

# create a cursor object
cursor = connection.cursor()

# execute a simple query to test the connection
cursor.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE NOT NULL);")


# Inserting data into the table
cursor.execute("INSERT INTO users (name, email) VALUES ('John Doe', 'mosaproject1@gmail.com');")

# Commit the changes
connection.commit()

# fetch and display the data
cursor.execute("SELECT * FROM users;")
rows = cursor.fetchall()


print("Users in the database:")
for row in rows:
    print(row)

# close the connection
connection.close()