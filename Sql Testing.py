import mysql.connector
import pandas as panda
import sqlalchemy as create_engine
from flask import Flask, render_template, request

# Create a connection handler to MySQL server and DB
conn = mysql.connector.connect(user='root', password='admin',
                              host='localhost',
                              database='tutorial')


"""
df=pd.read_excdl('NameNumbers.xlsx')
engine = create_engine('mysql://ryan:ryan@localhost/contacts')
df.to_sql('people',conn=engine, if_exists='append', index=False)
"""



def create():
    query= "CREATE TABLE IF NOT EXISTS Users(email STRING NOT NULL, password STRING NOT NULL, PRIMARY KEY(email)"
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS Users(
            email STRING NOT NULL, 
            password STRING NOT NULL, 
            PRIMARY KEY(email)
        )''')
    conn.commit()


def populate_tables():
    cur = conn.cursor()
    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Users.csv')
    users = panda.DataFrame(data)
    for row in users.itertuples():
        cur.execute('''
                    INSERT or IGNORE INTO Users(email, password)
                    VALUES(?,?)
                    ''',
                    (row.email,
                     row.password)
                    )
    conn.commit()


def revealUsers():
    conn = sqlite3.connect(curDir + "/d.db")
    cur = conn.cursor()
    print(cur.execute("SELECT * FROM Users"))
    rows = cur.fetchall()
    for row in rows:
        print(row)
    conn.close()


def show():
    cur.execute("SELECT * FROM Users")
    conn.commit()
    cur.execute("SELECT * FROM Buyers")
    conn.commit()
    cur.execute("SELECT * FROM Credit_Cards")
    conn.commit()
    cur.execute("SELECT * FROM Address")
    conn.commit()
    cur.execute("SELECT * FROM Zipcode_Info")
    conn.commit()
    cur.execute("SELECT * FROM Sellers")
    conn.commit()
    cur.execute("SELECT * FROM Local_Vendors")
    conn.commit()
    cur.execute("SELECT * FROM Categories")
    conn.commit()
    cur.execute("SELECT * FROM Product_Listings")
    conn.commit()
    cur.execute("SELECT * FROM Orders")
    conn.commit()
    cur.execute("SELECT * FROM Reviews")
    conn.commit()
    cur.execute("SELECT * FROM Rating")
    conn.commit()
    conn.close()


create()
populate_tables()
revealUsers()

def retrieveUsers():
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("SELECT email, password FROM Users")
    users = cur.fetchall()
    conn.close()
    return users


def insertUser(email, password):
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("INSERT INTO Users (email,password) VALUES (?,?)",
                (email, password))
    conn.commit()
    conn.close()


def retrieveAddress():
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("SELECT address_id, zipcode, street_num, street_name FROM Address")
    address = cur.fetchall()
    conn.close()
    return address


def insertAddress(address_id, zipcode, street_num, street_name):
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("INSERT INTO Address (address_id, zipcode, street_num, street_named) VALUES (?,?,?,?)",
                (address_id, zipcode, street_num, street_name))
    conn.commit()
    conn.close()


def retrieveBuyers():
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("SELECT email, first_name, last_name, gender, age, home_address_id, billing_address_id FROM Buyers")
    buyers = cur.fetchall()
    conn.close()
    return buyers


def insertBuyers(email, first_name, last_name, gender, age, home_address_id, billing_address_id):
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("INSERT INTO Buyers (email, first_name, last_name, gender, age, home_address_id, billing_address_id VALUES (?,?,?,?,?,?,?)",
                (email, first_name, last_name, gender, age, home_address, billing_address_id))
    conn.commit()
    conn.close()


def retrieveCategories():
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("SELECT parent_category, category_name FROM Categories")
    categories = cur.fetchall()
    conn.close()
    return categories


def insertCategories(parent_category, category_name):
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("INSERT INTO Categories (parent_category, category_name) VALUES (?,?)",
                (parent_category, category_name))
    conn.commit()
    conn.close()


def retrieveCredit_Cards():
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("SELECT credit_card_num, card_code, expire_month, expire_year, card_type, owner_email) FROM Credit_Cards")
    credit_cards = cur.fetchall()
    conn.close()
    return credit_cards


def insertCredit_Cards(credit_card_num, card_code, expire_month, expire_year, card_type, owner_email):
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("INSERT INTO Credit_Cards (credit_card_num, card_code, expire_month, expire_year, card_type, owner_email) VALUES (?,?,?,?,?,?)",
                (credit_card_num, card_code, expire_month, expire_year, card_type, owner_email))
    conn.commit()
    conn.close()


def retrieveLocal_Vendors():
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("SELECT email, business_name, business_address_id, customer_service_number FROM Local_Vendors")
    local_vendors = cur.fetchall()
    conn.close()
    return local_vendors


def insertLocal_Vendors(email, business_name, business_address_id, customer_service_number):
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("INSERT INTO Local_Vendors (email, business_name, business_address_id, customer_service_number) VALUES (?,?,?,?)",
                (email, business_name, business_address_id, customer_service_number))
    conn.commit()
    conn.close()


def retrieveOrders():
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("SELECT transaction_id, seller_email, listing_id, buyer_email, date, quantity, payment FROM Orders")
    orders = cur.fetchall()
    conn.close()
    return orders


def insertOrders(transaction_id, seller_email, listing_id, buyer_email, date, quantity, payment):
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("INSERT INTO Orders (transaction_id, seller_email, listing_id, buyer_email, date, quantity, payment) VALUES (?,?,?,?,?,?,?)",
                (transaction_id, seller_email, listing_id, buyer_email, date, quantity, payment))
    conn.commit()
    conn.close()


def retrieveProduct_Listing():
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("SELECT email, password FROM Product_Listing")
    product_listing = cur.fetchall()
    conn.close()
    return product_listing


def insertProduct_Listing(email, password):
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("INSERT INTO Product_Listing (email, password) VALUES (?,?)",
                (email, password))
    conn.commit()
    conn.close()


def retrieveRating():
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("SELECT buyer_email, seller_email, date, rating, rating_desc FROM Rating")
    rating = cur.fetchall()
    conn.close()
    return rating


def insertRating(buyer_email, seller_email, date, rating, rating_desc):
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("INSERT INTO Rating (buyer_email, seller_email, date, rating, rating_desc) VALUES (?,?,?,?,?)",
                (buyer_email, seller_email, date, rating, rating_desc))
    conn.commit()
    conn.close()


def retrieveReviews():
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("SELECT buyer_email, seller_email, date, rating_desc FROM Reviews")
    reviews = cur.fetchall()
    conn.close()
    return reviews


def insertReviews(buyer_email, seller_email, date, rating_desc):
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("INSERT INTO Reviews (buyer_email, seller_email, date, rating_desc) VALUES (?,?,?,?)",
                (buyer_email, seller_email, date, rating_desc))
    conn.commit()
    conn.close()


def retrieveSellers():
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("SELECT email, routing_number, account_number, balance FROM Sellers")
    sellers = cur.fetchall()
    conn.close()
    return sellers


def insertSellers(email, routing_number, account_number, balance):
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("INSERT INTO Sellers (email, routing_number, account_number, balance) VALUES (?,?,?,?)",
                (email, routing_number, account_number, balance))
    conn.commit()
    conn.close()


def retrieveZipcode_Info():
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("SELECT zipcode, city, state_id, population, density, county_name, timezone FROM Zipcode_Info")
    zipcode_info = cur.fetchall()
    conn.close()
    return zipcode_info


def insertZipcode_Info(zipcode, city, state_id, population, density, county_name, timezone):
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("INSERT INTO Zipcode_Info (zipcode, city, state_id, population, density, county_name, timezone) VALUES (?,?)",
                (zipcode, city, state_id, population, density, county_name, timezone))
    conn.commit()
    conn.close()
