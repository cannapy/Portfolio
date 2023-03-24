import express from "express"
import mysql from "mysql"
import cors from 'cors'
// 1: npm init-y
// 2: npm i express mysql nodemon body-parser morgan cors
// 3: In package.json -> scripts, write: "start": "nodemon index.js"
// 4: In package.json below main, write: "type": "module",
// npm start

// IF THERE IS A AUTH PROBLEM
// ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'password';


const app = express()
app.use(express.json())
app.use(cors())

const db = mysql.createConnection({
    host: "localhost",
    user: "root",
    password: "abc123",
    database: "test"
})

app.get("/", (req, res) => {
    res.json("Hello this is the backend")
})

app.get("/books", (req, res) => {
    const q = "SELECT * FROM test.BOOK"
    db.query(q, (err, data) => {
        if (err) return res.json(err)
        return res.json(data)
    })
})


//User sends the book data
app.post("/add", (req, res) => {
    const q = "INSERT INTO test.BOOK (`Title`, `Desc`, `Cover`, `Price`) VALUES (?)"
    const values = [
        req.body.Title,
        req.body.Desc,
        req.body.Cover,
        req.body.Price,
    ]

    db.query(q, [values], (err, data) => {
        if (err) return res.send(err)
        return res.json("Book has been created successfully")
    })
})

app.delete("/books/:id", (req, res) => {
    const bookId = req.params.id
    const q = "DELETE FROM BOOK WHERE Id = ?"

    db.query(q, [bookId], (err, data) => {
        if (err) return res.send(err)
        return res.json("Book has been deleted successfully.")
    })
})

app.put("/update/:Id", (req, res) => {
    const bookId = req.params.Id
    const q = 'UPDATE test.book SET `Title`=?, `Desc`=?, `Cover`=?, `Price`=? WHERE `Id` =?';
    const values = [
        req.body.Title,
        req.body.Desc,
        req.body.Cover,
        req.body.Price,
    ]

    db.query(q, [...values, bookId], (err, data) => {
        if (err) return res.send(err)
        return res.json(data)
    })
})

app.listen(8800, () => {
    console.log("Connected to backend!")
})