import sqlite3
import mysql.connector
import pandas as panda
import os
from flask import Flask, render_template, request

curDir = os.path.dirname(os.path.abspath(__file__))


def create():
    conn = sqlite3.connect(curDir + "/database.db")
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS Users(
            email STRING NOT NULL, 
            password STRING NOT NULL, 
            PRIMARY KEY(email)
        )''')
    conn.commit()

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

    cur.execute('''CREATE TABLE IF NOT EXISTS Credit_Cards(
            credit_card_num INTEGER NOT NULL, 
            card_code INTEGER NOT NULL, 
            expire_month INTEGER NOT NULL, 
            expire_year INTEGER NOT NULL, 
            card_type STRING NOT NULL, 
            owner_email STRING NOT NULL, 
            PRIMARY KEY (credit_card_num)
        )''')
    conn.commit()

    cur.execute('''CREATE TABLE IF NOT EXISTS Address(
            address_id INTEGER NOT NULL, 
            zipcode INTEGER NOT NULL, 
            street_num INTEGER NOT NULL, 
            street_name STRING NOT NULL, 
            PRIMARY KEY(address_id)
        )''')
    conn.commit()

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

    cur.execute('''CREATE TABLE IF NOT EXISTS Sellers(
            email STRING NOT NULL, 
            routing_number INTEGER NOT NULL, 
            account_number INTEGER NOT NULL, 
            balance FLOAT NOT NULL, 
            PRIMARY KEY(email)
        )''')
    conn.commit()

    cur.execute('''CREATE TABLE IF NOT EXISTS Local_Vendors(
            email STRING NOT NULL, 
            business_name STRING NOT NULL, 
            business_address_id INTEGER NOT NULL, 
            customer_service_number INTEGER NOT NULL, 
            PRIMARY KEY(email)
        )''')
    conn.commit()

    cur.execute('''CREATE TABLE IF NOT EXISTS Categories(
            parent_category STRING NOT NULL, 
            category_name STRING NOT NULL, 
            PRIMARY KEY(category_name)
        )''')
    conn.commit()

    cur.execute('''CREATE TABLE IF NOT EXISTS Product_Listings(
            seller_email STRING NOT NULL,
            listing_id INTEGER NOT NULL, 
            category STRING NOT NULL,
            title STRING NOT NULL,
            product_name STRING NOT NULL,
            product_description STRING NOT NULL, 
            price INTEGER NOT NULL, 
            quantity INTEGER NOT NULL, 
            PRIMARY KEY(seller_email,listing_id)
        )''')
    conn.commit()

    cur.execute('''CREATE TABLE IF NOT EXISTS Orders(
            transaction_id INTEGER NOT NULL, 
            seller_email STRING NOT NULL, 
            buyer_email STRING NOT NULL, 
            date DATE NOT NULL, 
            quantity INTEGER NOT NULL, 
            payment INTEGER NOT NULL, 
            PRIMARY KEY(transaction_id)
        )''')
    conn.commit()

    cur.execute('''CREATE TABLE IF NOT EXISTS Reviews(
            buyer_email STRING NOT NULL, 
            seller_email STRING NOT NULL, 
            listing_id INTEGER NOT NULL, 
            review_desc STRING NOT NULL, 
            PRIMARY KEY(buyer_email, seller_email, listing_id)
        )''')
    conn.commit()

    cur.execute('''CREATE TABLE IF NOT EXISTS Rating(
            buyer_email STRING NOT NULL, 
            seller_email STRING NOT NULL, 
            date DATE NOT NULL, 
            rating FLOAT NOT NULL, 
            rating_desc STRING NOT NULL, 
            PRIMARY KEY(buyer_email, seller_email, date)
        )''')
    conn.commit()

    # Import Excel Statements
    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Address.csv')
    address = panda.DataFrame(data)
    for row in address.itertuples():
        cur.execute('''
                INSERT INTO Address(address_id, zipcode, street_num, street_name)
                VALUES(?,?,?,?)
                    ''',
                    (row.address_id,
                    row.zipcode,
                    row.street_num,
                    row.street_name)
                    )
    conn.commit()

    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Buyers.csv')
    buyers = panda.DataFrame(data)
    for row in buyers.itertuples():
        cur.execute('''
                    INSERT INTO Buyers(email, first_name, last_name, gender, age, home_address_id, billing_address_id)
                    VALUES(?,?,?,?,?,?,?)
                    ''',
                    row.email,
                    row.first_name,
                    row.last_name,
                    row.gender,
                    row.age,
                    row.home_address_id,
                    row.billing_address_id
                    )
    conn.commit()

    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Categories.csv')
    categories = panda.DataFrame(data)
    for row in categories.itertuples():
        cur.execute('''
                    INSERT INTO Categories(parent_category, category_name)
                    VALUES(?,?)
                    ''',
                    row.parent_category,
                    row.category_name
                    )
    conn.commit()

    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Credit_Cards.csv')
    credit_cards = panda.DataFrame(data)
    for row in credit_cards.itertuples():
        cur.execute('''
                    INSERT INTO Credit_Cards(credit_card_num, card_code, expire_month, expire_year, card_type, owner_email)
                    VALUES(?,?,?,?,?,?)
                    ''',
                    row.credit_card_num,
                    row.card_code,
                    row.expire_month,
                    row.expire_year,
                    row.card_type,
                    row.owner_email
                    )
    conn.commit()

    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Local_Vendors.csv')
    local_vendors = panda.DataFrame(data)
    for row in local_vendors.itertuples():
        cur.execute('''
                    INSERT INTO Local_Vendors(email, business_name, business_address_id, customer_service_number)
                    VALUES(?,?,?,?)
                    ''',
                    row.email,
                    row.business_name,
                    row.business_address_id,
                    row.customer_service_number
                    )
    conn.commit()

    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Orders.csv')
    orders = panda.DataFrame(data)
    for row in orders.itertuples():
        cur.execute('''
                    INSERT INTO Orders(transaction_id, seller_email, listing_id, buyer_email, date, quantity, payment)
                    VALUES(?,?,?,?,?,?,?)
                    ''',
                    row.transaction_id,
                    row.seller_email,
                    row.listing_id,
                    row.buyer_email,
                    row.date,
                    row.quantity,
                    row.payment
                    )
    conn.commit()

    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Product_Listing.csv')
    product_listing = panda.DataFrame(data)
    for row in product_listing.itertuples():
        cur.execute('''
                    INSERT INTO Product_Listing(seller_email, listing_id, category, title, product_name, product_description, price, quantity)
                    VALUES(?,?,?,?,?,?,?,?)
                    ''',
                    row.seller_email,
                    row.listing_id,
                    row.category,
                    row.title,
                    row.product_name,
                    row.product_description,
                    row.price,
                    row.quantity
                    )
    conn.commit()

    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Ratings.csv')
    rating = panda.DataFrame(data)
    for row in rating.itertuples():
        cur.execute('''
                    INSERT INTO Rating(buyer_email, seller_email, date, rating, rating_desc)
                    VALUES(?,?,?,?,?)
                    ''',
                    row.buyer_email,
                    row.seller_email,
                    row.date,
                    row.rating,
                    row.rating_desc
                    )
    conn.commit()

    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Reviews.csv')
    reviews = panda.DataFrame(data)
    for row in reviews.itertuples():
        cur.execute('''
                    INSERT INTO Reviews(buyer_email, seller_email, date, rating_desc)
                    VALUES(?,?,?,?)
                    ''',
                    row.buyer_email,
                    row.seller_email,
                    row.date,
                    row.rating_desc
                    )
    conn.commit()

    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Sellers.csv')
    sellers = panda.DataFrame(data)
    for row in sellers.itertuples():
        cur.execute('''
                    INSERT INTO Sellers(email, routing_number, account_number, balance)
                    VALUES(?,?,?,?)
                    ''',
                    row.email,
                    row.routing_number,
                    row.account_number,
                    row.balance
                    )
    conn.commit()

    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Users.csv')
    users = panda.DataFrame(data)
    for row in users.itertuples():
        cur.execute('''
                    INSERT INTO Users(email, password)
                    VALUES(?,?)
                    ''',
                    row.email,
                    row.password
                    )
    conn.commit()

    data = panda.read_csv(r'C:\Users\dmoye\OneDrive\Desktop\NMData\Zipcode_Info.csv')
    zipcode_info = panda.DataFrame(data)
    for row in zipcode_info.itertuples():
        cur.execute('''
                    INSERT INTO Zipcode_Info(zipcode, city, state_id, population, density, county_name, timezone)
                    VALUES(?,?,?,?,?,?,?)
                    ''',
                    row.zipcode,
                    row.city,
                    row.state_id,
                    row.population,
                    row.density,
                    row.county_name,
                    row.timezone
                    )
    conn.commit()

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
