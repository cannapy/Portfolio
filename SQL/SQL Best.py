import sqlite3

#import mysql.connector as sqlite3

import pandas as panda
import os


from flask import Flask, render_template, request

curDir = os.path.dirname(os.path.abspath(__file__))

# hash function with python library to hash password


def create():
    conn = sqlite3.connect(curDir + "/d.db")
    cur = conn.cursor()

    cur.execute('''CREATE TABLE IF NOT EXISTS Users(
            email STRING NOT NULL, 
            password STRING NOT NULL, 
            PRIMARY KEY(email)
        )''')
    conn.commit()

    conn = sqlite3.connect(curDir + "/1d.db")
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS Buyers(
            email STRING NOT NULL, 
            first_name STRING NOT NULL, 
            last_name STRING NOT NULL, 
            gender STRING NOT NULL, 
            age INTEGER NOT NULL, 
            home_address_id INTEGER NOT NULL, 
            billing_address_id INTEGER NOT NULL, 
            PRIMARY KEY (email)
        )''')
    conn.commit()

    conn = sqlite3.connect(curDir + "/da.db")
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS Credit_Cards(
                credit_card_num STRING NOT NULL, 
                card_code INTEGER NOT NULL, 
                expire_month INTEGER NOT NULL, 
                expire_year INTEGER NOT NULL, 
                card_type STRING NOT NULL, 
                Owner_email STRING NOT NULL, 
                PRIMARY KEY (credit_card_num)
            )''')
    conn.commit()

    conn = sqlite3.connect(curDir + "/2d.db")
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS Address(
                address_id STRING NOT NULL, 
                zipcode INTEGER NOT NULL, 
                street_num INTEGER NOT NULL, 
                street_name STRING NOT NULL, 
                PRIMARY KEY(address_id)
            )''')
    conn.commit()

    conn = sqlite3.connect(curDir + "/dat.db")
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS Zipcode_Info(
                zipcode INTEGER NOT NULL, 
                city STRING NOT NULL, 
                state_id INTEGER NOT NULL, 
                population INTEGER NOT NULL, 
                density FLOAT NOT NULL, 
                county_name STRING NOT NULL, 
                timezone STRING NOT NULL, 
                PRIMARY KEY(zipcode)
            )''')
    conn.commit()

    conn = sqlite3.connect(curDir + "/3d.db")
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS Sellers(
                email STRING NOT NULL, 
                routing_number INTEGER NOT NULL, 
                account_number INTEGER NOT NULL, 
                balance FLOAT NOT NULL, 
                PRIMARY KEY(email)
            )''')
    conn.commit()

    conn = sqlite3.connect(curDir + "/data.db")
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS Local_Vendors(
                Email STRING NOT NULL, 
                Business_Name STRING NOT NULL, 
                Business_Address_ID INTEGER NOT NULL, 
                Customer_Service_Number INTEGER NOT NULL, 
                PRIMARY KEY(Email)
            )''')
    conn.commit()

    conn = sqlite3.connect(curDir + "/4d.db")
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS Categories(
                parent_category STRING NOT NULL, 
                category_name STRING NOT NULL, 
                PRIMARY KEY(category_name)
            )''')
    conn.commit()

    conn = sqlite3.connect(curDir + "/datab.db")
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS Product_Listing(
                Seller_Email STRING NOT NULL,
                Listing_ID INTEGER NOT NULL, 
                Category STRING NOT NULL,
                Title STRING NOT NULL,
                Product_Name STRING NOT NULL,
                Product_Description STRING NOT NULL, 
                Price STRING NOT NULL, 
                Quantity INTEGER NOT NULL, 
                PRIMARY KEY(Seller_Email,Listing_ID)
            )''')
    conn.commit()

    conn = sqlite3.connect(curDir + "/5d.db")
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS Orders(
                Transaction_ID INTEGER NOT NULL, 
                Seller_Email STRING NOT NULL,
                Listing_ID INTEGER NOT NULL, 
                Buyer_Email STRING NOT NULL, 
                Date DATE NOT NULL, 
                Quantity INTEGER NOT NULL, 
                Payment INTEGER NOT NULL, 
                PRIMARY KEY(Transaction_ID)
            )''')
    conn.commit()

    conn = sqlite3.connect(curDir + "/databa.db")
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS Reviews(
                Buyer_Email STRING NOT NULL, 
                Seller_Email STRING NOT NULL, 
                Listing_ID INTEGER NOT NULL, 
                Review_Desc STRING NOT NULL, 
                PRIMARY KEY(Buyer_Email, Seller_Email, Listing_ID)
            )''')
    conn.commit()

    conn = sqlite3.connect(curDir + "/6d.db")
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS Ratings(
                Buyer_Email STRING NOT NULL, 
                Seller_Email STRING NOT NULL, 
                Date DATE NOT NULL, 
                Rating FLOAT NOT NULL, 
                Rating_Desc STRING NOT NULL, 
                PRIMARY KEY(Buyer_Email, Seller_Email, Date)
            )''')
    conn.commit()


def populate_tables():
    conn = sqlite3.connect(curDir + "/d.db")
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

    conn = sqlite3.connect(curDir + "/1d.db")
    cur = conn.cursor()
    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Buyers.csv')
    buyers = panda.DataFrame(data)
    for row in buyers.itertuples():
        # ignore duplicate email addresses
        cur.execute('''
                    INSERT or IGNORE INTO Buyers(email, first_name, last_name, gender, age, home_address_id, billing_address_id)
                    VALUES(?,?,?,?,?,?,?)
                    ''',
                    (row.email,
                     row.first_name,
                     row.last_name,
                     row.gender,
                     row.age,
                     row.home_address_id,
                     row.billing_address_id)
                    )
    conn.commit()

    conn = sqlite3.connect(curDir + "/da.db")
    cur = conn.cursor()
    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Credit_Cards.csv')
    credit_cards = panda.DataFrame(data)
    for row in credit_cards.itertuples():
        cur.execute('''
                    INSERT or IGNORE INTO Credit_Cards(credit_card_num, card_code, expire_month, expire_year, card_type, Owner_email)
                    VALUES(?,?,?,?,?,?)
                    ''',
                    (row.credit_card_num,
                     row.card_code,
                     row.expire_month,
                     row.expire_year,
                     row.card_type,
                     row.Owner_email)
                    )
    conn.commit()

    # Import Excel Statements

    conn = sqlite3.connect(curDir + "/2d.db")
    cur = conn.cursor()
    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Address.csv')
    address = panda.DataFrame(data)
    for row in address.itertuples():
        cur.execute('''
                    INSERT or IGNORE INTO Address(address_id, zipcode, street_num, street_name)
                    VALUES(?,?,?,?)
                    ''',
                    (row.address_id,
                     row.zipcode,
                     row.street_num,
                     row.street_name)
                    )
    conn.commit()

    conn = sqlite3.connect(curDir + "/dat.db")
    cur = conn.cursor()
    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Zipcode_Info.csv')
    zipcode_info = panda.DataFrame(data)
    for row in zipcode_info.itertuples():
        cur.execute('''
                    INSERT or IGNORE INTO Zipcode_Info(zipcode, city, state_id, population, density, county_name, timezone)
                    VALUES(?,?,?,?,?,?,?)
                    ''',
                    (row.zipcode,
                     row.city,
                     row.state_id,
                     row.population,
                     row.density,
                     row.county_name,
                     row.timezone)
                    )
    conn.commit()

    conn = sqlite3.connect(curDir + "/3d.db")
    cur = conn.cursor()
    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Sellers.csv')
    sellers = panda.DataFrame(data)
    for row in sellers.itertuples():
        cur.execute('''
                    INSERT or IGNORE INTO Sellers(email, routing_number, account_number, balance)
                    VALUES(?,?,?,?)
                    ''',
                    (row.email,
                     row.routing_number,
                     row.account_number,
                     row.balance)
                    )
    conn.commit()

    conn = sqlite3.connect(curDir + "/data.db")
    cur = conn.cursor()
    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Local_Vendors.csv')
    local_vendors = panda.DataFrame(data)
    for row in local_vendors.itertuples():
        cur.execute('''
                    INSERT or IGNORE INTO Local_Vendors(Email, Business_Name, Business_Address_ID, Customer_Service_Number)
                    VALUES(?,?,?,?)
                    ''',
                    (row.Email,
                     row.Business_Name,
                     row.Business_Address_ID,
                     row.Customer_Service_Number)
                    )
    conn.commit()

    conn = sqlite3.connect(curDir + "/4d.db")
    cur = conn.cursor()
    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Categories.csv')
    categories = panda.DataFrame(data)
    for row in categories.itertuples():
        cur.execute('''
                    INSERT or IGNORE INTO Categories(parent_category, category_name)
                    VALUES(?,?)
                    ''',
                    (row.parent_category,
                     row.category_name)
                    )
    conn.commit()

    conn = sqlite3.connect(curDir + "/datab.db")
    cur = conn.cursor()
    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Product_Listing.csv')
    product_listing = panda.DataFrame(data)
    for row in product_listing.itertuples():
        cur.execute('''
                    INSERT or IGNORE INTO 
                    Product_Listing(Seller_Email, Listing_ID, Category, Title, Product_Name, Product_Description, Price, Quantity)
                    VALUES(?,?,?,?,?,?,?,?)
                    ''',
                    (row.Seller_Email,
                     row.Listing_ID,
                     row.Category,
                     row.Title,
                     row.Product_Name,
                     row.Product_Description,
                     row.Price,
                     row.Quantity)
                    )
    conn.commit()

    conn = sqlite3.connect(curDir + "/5d.db")
    cur = conn.cursor()
    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Orders.csv')
    orders = panda.DataFrame(data)
    for row in orders.itertuples():
        cur.execute('''
                    INSERT or IGNORE INTO
                    Orders(Transaction_ID, Seller_Email, Listing_ID, Buyer_Email, Date, Quantity, Payment)
                    VALUES(?,?,?,?,?,?,?)
                    ''',
                    (row.Transaction_ID,
                     row.Seller_Email,
                     row.Listing_ID,
                     row.Buyer_Email,
                     row.Date,
                     row.Quantity,
                     row.Payment)
                    )
    conn.commit()

    conn = sqlite3.connect(curDir + "/databa.db")
    cur = conn.cursor()
    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Ratings.csv')
    rating = panda.DataFrame(data)
    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Reviews.csv')
    reviews = panda.DataFrame(data)
    for row in reviews.itertuples():
        cur.execute('''
                    INSERT or IGNORE INTO Reviews(Buyer_Email, Seller_Email, Listing_ID, Review_Desc)
                    VALUES(?,?,?,?)
                    ''',
                    (row.Buyer_Email,
                     row.Seller_Email,
                     row.Listing_ID,
                     row.Review_Desc)
                    )
    conn.commit()

    conn = sqlite3.connect(curDir + "/6d.db")
    cur = conn.cursor()
    for row in rating.itertuples():
        cur.execute('''
                    INSERT or IGNORE INTO Ratings(Buyer_Email, Seller_Email, Date, Rating, Rating_Desc)
                    VALUES(?,?,?,?,?)
                    ''',
                    (row.Buyer_Email,
                     row.Seller_Email,
                     row.Date,
                     row.Rating,
                     row.Rating_Desc)
                    )
    conn.commit()

    conn = sqlite3.connect(curDir + "/d.db")
    cur = conn.cursor()
    print(cur.execute("SELECT * FROM Users"))
    rows = cur.fetchall()
    for row in rows:
        print(row)
    conn.close()

    conn = sqlite3.connect(curDir + "/1d.db")
    cur = conn.cursor()
    print(cur.execute("SELECT * FROM Buyers"))
    rows = cur.fetchall()
    for row in rows:
        print(row)

    conn = sqlite3.connect(curDir + "/da.db")
    cur = conn.cursor()
    rows = cur.execute("SELECT * FROM Credit_Cards").fetchall()
    for row in rows:
        print(row)
    conn.close()

    conn = sqlite3.connect(curDir + "/2d.db")
    cur = conn.cursor()
    print(cur.execute("SELECT * FROM Address"))
    rows = cur.fetchall()
    for row in rows:
        print(row)
    conn.close()

    conn = sqlite3.connect(curDir + "/dat.db")
    cur = conn.cursor()
    print(cur.execute("SELECT * FROM Zipcode_Info"))
    rows = cur.fetchall()
    for row in rows:
        print(row)
    conn.close()

    conn = sqlite3.connect(curDir + "/3d.db")
    cur = conn.cursor()
    print(cur.execute("SELECT * FROM Sellers"))
    rows = cur.fetchall()
    for row in rows:
        print(row)
    conn.close()

    conn = sqlite3.connect(curDir + "/data.db")
    cur = conn.cursor()
    print(cur.execute("SELECT * FROM Local_Vendors"))
    rows = cur.fetchall()
    for row in rows:
        print(row)
    conn.close()

    conn = sqlite3.connect(curDir + "/4d.db")
    cur = conn.cursor()
    print(cur.execute("SELECT * FROM Categories"))
    rows = cur.fetchall()
    for row in rows:
        print(row)
    conn.close()

    conn = sqlite3.connect(curDir + "/datab.db")
    cur = conn.cursor()
    print(cur.execute("SELECT * FROM Product_Listing"))
    rows = cur.fetchall()
    for row in rows:
        print(row)
    conn.close()

    conn = sqlite3.connect(curDir + "/5d.db")
    cur = conn.cursor()
    print(cur.execute("SELECT * FROM Orders"))
    rows = cur.fetchall()
    for row in rows:
        print(row)
    conn.close()

    conn = sqlite3.connect(curDir + "/databa.db")
    cur = conn.cursor()
    print(cur.execute("SELECT * FROM Reviews"))
    rows = cur.fetchall()
    for row in rows:
        print(row)
    conn.close()

    conn = sqlite3.connect(curDir + "/6d.db")
    cur = conn.cursor()
    print(cur.execute("SELECT * FROM Ratings"))
    rows = cur.fetchall()
    for row in rows:
        print(row)
    conn.close()


create()
populate_tables()

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
    cur.execute("SELECT email, first_name, last_name, gender, age, home_address, billing_address FROM Buyers")
    buyers = cur.fetchall()
    conn.close()
    return buyers


def insertBuyers(email, first_name, last_name, gender, age, home_address, billing_address):
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO Buyers (email, first_name, last_name, gender, age, home_address, billing_address) VALUES (?,?,?,?,?,?,?)",
        (email, first_name, last_name, gender, age, home_address, billing_address))
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
    cur.execute(
        "SELECT credit_card_num, card_code, expire_month, expire_year, card_type, Owner_email) FROM Credit_Cards")
    credit_cards = cur.fetchall()
    conn.close()
    return credit_cards


def insertCredit_Cards(credit_card_num, card_code, expire_month, expire_year, card_type, owner_email):
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO Credit_Cards (credit_card_num, card_code, expire_month, expire_year, card_type, Owner_email) VALUES (?,?,?,?,?,?)",
        (credit_card_num, card_code, expire_month, expire_year, card_type, owner_email))
    conn.commit()
    conn.close()


def retrieveLocal_Vendors():
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("SELECT Email, Business_Name, Business_Address_ID, Customer_Service_Number FROM Local_Vendors")
    local_vendors = cur.fetchall()
    conn.close()
    return local_vendors


def insertLocal_Vendors(email, business_name, business_address_id, customer_service_number):
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO Local_Vendors (Email, Business_Name, Business_Address_ID, Customer_Service_Number) VALUES (?,?,?,?)",
        (email, business_name, business_address_id, customer_service_number))
    conn.commit()
    conn.close()


def retrieveOrders():
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("SELECT Transaction_ID, Seller_Email, Listing_ID, Buyer_Email, Date, Quantity, Payment FROM Orders")
    orders = cur.fetchall()
    conn.close()
    return orders


def insertOrders(Transaction_ID, Seller_Email, Listing_ID, Buyer_Email, Date, Quantity, Payment):
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO Orders ("
        "Transaction_ID, Seller_Email, Listing_ID, Buyer_Email, Date, Quantity, Payment) VALUES (?,?,?,?,?,?,?)",
        (Transaction_ID, Seller_Email, Listing_ID, Buyer_Email, Date, Quantity, Payment))
    conn.commit()
    conn.close()


def retrieveProduct_Listing():
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute(
        "SELECT Seller_Email, Listing_ID, Category, Title, Product_Name, Product_Description, Price, Quantity FROM Product_Listing")
    product_listing = cur.fetchall()
    conn.close()
    return product_listing


def insertProduct_Listing(Seller_Email, Listing_ID, Category, Title, Product_Name, Product_Description, Price,
                          Quantity):
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO Product_Listing(Seller_Email, Listing_ID, Category, Title, Product_Name, Product_Description, Price, Quantity) VALUES (?,?,?,?,?,?,?,?)",
        (Seller_Email, Listing_ID, Category, Title, Product_Name, Product_Description, Price, Quantity))
    conn.commit()
    conn.close()


def retrieveRating():
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("SELECT Buyer_Email, Seller_Email, Date, Rating, Rating_Desc FROM Rating")
    rating = cur.fetchall()
    conn.close()
    return rating


def insertRating(buyer_email, seller_email, date, rating, rating_desc):
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("INSERT INTO Ratings(Buyer_Email, Seller_Email, Date, Rating, Rating_Desc) VALUES (?,?,?,?,?)",
                (Buyer_Email, Seller_Email, Date, Rating, Rating_Desc))
    conn.commit()
    conn.close()


def retrieveReviews():
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("SELECT Buyer_Email, Seller_Email, Listing_ID, Review_Desc FROM Reviews")
    reviews = cur.fetchall()
    conn.close()
    return reviews


def insertReviews(Buyer_Email, Seller_Email, Date, Review_Desc):
    conn = sqlite3.connect("database.db")
    cur = conn.cursor()
    cur.execute("INSERT INTO Reviews (Buyer_Email, Seller_Email, Listing_ID, Review_Desc) VALUES (?,?,?,?)",
                (Buyer_Email, Seller_Email, Date, Review_Desc))
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
    cur.execute(
        "INSERT INTO Zipcode_Info (zipcode, city, state_id, population, density, county_name, timezone) VALUES (?,?)",
        (zipcode, city, state_id, population, density, county_name, timezone))
    conn.commit()
    conn.close()
