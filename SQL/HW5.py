import sqlite3
import os

curDir = os.path.dirname(os.path.abspath(__file__))


def create():
    conn = sqlite3.connect(curDir + "/HW5.db")
    cur = conn.cursor()
    print("10")
    cur.execute('''CREATE TABLE IF NOT EXISTS Emp(
            eid INT NOT NULL,
            ename STRING NOT NULL,
            age INT NOT NULL,
            salary REAL NOT NULL,
            PRIMARY KEY(eid)
        )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS Works(
            eid INT NOT NULL,
            did INT NOT NULL,
            pct_time INT NOT NULL,
            PRIMARY KEY(eid, did)
        )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS Dept(
            did INT NOT NULL,
            budget REAL NOT NULL,
            managerid INT NOT NULL,
            PRIMARY KEY(did)
        )''')

    print("created")


def implement():
    conn = sqlite3.connect(curDir + "/HW5.db")
    cur = conn.cursor()
    with open('Homework5-dataset-sp2022.txt', 'r') as y:
        # start at 3rd line
        lines = y.readlines()
        x = 0
        txt = ""

        for line in lines:
            if x < 62:
                if x < 2:
                    line = ""
                    txt = ""
                    pass

                elif x == 2:
                    txt = "1, 'Jim', 33, 70000.0"
                    cur.execute('''INSERT OR IGNORE INTO Emp VALUES ({})'''.format(txt))

                elif x == 50:
                    txt = "49, 'Ellen', 20, 60000.0"
                    cur.execute('''INSERT OR IGNORE INTO Emp VALUES ({})'''.format(txt))

                else:
                    txt = line
                    cur.execute('''INSERT OR IGNORE INTO Emp VALUES ({})'''.format(txt))

            if 62 <= x <= 165:
                if x < 66:
                    line = ""
                    txt = ""
                    pass

                if x >= 66:
                    txt = line
                    cur.execute('''INSERT OR IGNORE INTO Works VALUES ({})'''.format(txt))

            if 166 <= x <= 180:
                if x < 170:
                    line = ""
                    txt = ""
                    pass

                if x >= 170:
                    txt = line
                    cur.execute('''INSERT OR IGNORE INTO Dept VALUES ({})'''.format(txt))

            print("line")
            print(line)
            print('t')
            print(txt)
            print(x)
            print("\n\n")

            x += 1

    conn.commit()
    conn.close()


def q1():
    conn = sqlite3.connect(curDir + "/HW5.db")
    cur = conn.cursor()

    print("\n\nQ1")
    for row in cur.execute('''
                            SELECT Empt.ename as name, Empt.salary as salary
                            FROM Emp as Empt
                            JOIN Works as workt on Empt.eid = Workt.eid
                            WHERE workt.did=0 and workt.did=2
                            '''):
        print(row)
    conn.commit()
    conn.close()


# 2) Find the name, age and salary of the youngest employee in each department.
def q2():
    conn = sqlite3.connect(curDir + "/HW5.db")
    cur = conn.cursor()
    print("\n\nQ2")
    for row in cur.execute(''' SELECT did as did, empt.ename as name, empt.age as age, empt.salary as salary
                            FROM (SELECT * From Emp ORDER BY age ASC) empt
                            JOIN ( select * FROM Works ORDER BY did) workt 
                            on empt.eid = workt.eid
                            GROUP BY workt.did
                            '''):
        print(row)
    conn.commit()
    conn.close()
    pass


# 3) Find the names of managers who manage the departments with the largest budget.
# How many departments are we looking for? TOP 3?
def q3():
    conn = sqlite3.connect(curDir + "/HW5.db")
    print("\n\nQ3")
    cur = conn.cursor()
    for row in cur.execute(''' 
                        SELECT empt.ename as name, dep.managerid, dep.did as department, dep.budget as budget
                        FROM Emp as empt
                        JOIN Works as workt on empt.eid=workt.eid 
                        JOIN (SELECT * FROM Dept ORDER BY Budget desc) dep on workt.did=dep.did
                        GROUP BY dep.did
                        ORDER BY dep.budget desc
                        LIMIT 3'''):
        print(row)
    conn.commit()
    conn.close()


# 4) Find the department(s) with the highest average salary of employees.
def q4():
    conn = sqlite3.connect(curDir + "/HW5.db")
    print("\n\nQ4")
    cur = conn.cursor()
    for row in cur.execute('''SELECT avg(salary) as avg_sal, workt.did as did
                        FROM Emp as empt
                        JOIN Works as workt on empt.eid=workt.eid 
                        GROUP BY workt.did
                        order by avg_sal desc
                        limit 10'''):
        print(row)

    conn.commit()
    conn.close()


# 5) Find the employees (i.e., include mangers) with a salary greater than some manager.
def q5():
    conn = sqlite3.connect(curDir + "/HW5.db")
    print("\n\nQ5")
    cur = conn.cursor()
    for row in cur.execute('''SELECT Emp.ename, Emp.salary
                        FROM Emp, (Select salary as low_man, empt.ename
                                    FROM Emp as empt
                                    JOIN Dept as dept on empt.eid=dept.managerid
                                    ORDER BY empt.salary ASC
                                    LIMIT 1
                                 ) newt
                        WHERE Emp.salary > newt.low_man
                        ORDER BY Emp.salary DESC
                        '''):
        print(row)
    conn.commit()
    conn.close()


try:
    f = open('HW5.db', 'r')
    implement()
    f.close()
except FileNotFoundError:
    create()
    implement()

q1()
q2()
q3()
q4()
q5()
