package main

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/syaiful6/goduckdb"
)

var (
	db *sql.DB
)

type user struct {
	name    string
	age     int
	height  float32
	awesome bool
	bday    time.Time
}

func main() {
	// Use second argument to store DB on disk
	// db, err := sql.Open("duckdb", "foobar.db")

	var err error
	db, err = sql.Open("duckdb", "duckdb.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	check(db.Ping())

	check(db.Exec("CREATE TABLE users(name VARCHAR, age INTEGER, height FLOAT, awesome BOOLEAN, bday DATE)"))
	check(db.Exec("INSERT INTO users VALUES('marc', 99, 1.91, true, '1970-01-01')"))
	check(db.Exec("INSERT INTO users VALUES('macgyver', 70, 1.85, true, '1951-01-23')"))

	rows, err := db.Query(`
		SELECT name, age, height, awesome, bday
		FROM users 
		WHERE (name = ? OR name = ?) AND age > ? AND awesome = ?`,
		"macgyver", "marc", 30, true,
	)
	check(err)
	defer rows.Close()

	for rows.Next() {
		u := new(user)
		err := rows.Scan(&u.name, &u.age, &u.height, &u.awesome, &u.bday)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf(
			"%s is %d years old, %.2f tall, bday on %s and has awesomeness: %t\n",
			u.name, u.age, u.height, u.bday.Format(time.RFC3339), u.awesome,
		)
	}
	check(rows.Err())

	res, err := db.Exec("DELETE FROM users")
	check(err)

	ra, _ := res.RowsAffected()
	log.Printf("Deleted %d rows\n", ra)

	runTransaction()
	testPreparedStmt()
	testPurchase()
	openWithReadOnly()
}

func check(args ...interface{}) {
	err := args[len(args)-1]
	if err != nil {
		panic(err)
	}
}

func runTransaction() {
	log.Println("Starting transaction...")
	tx, err := db.Begin()
	check(err)

	check(tx.Exec("INSERT INTO users VALUES('gru', 25, 1.35, false, '1996-04-03')"))
	row := tx.QueryRow("SELECT COUNT(*) FROM users WHERE name = ?", "gru")
	var count int64
	check(row.Scan(&count))
	if count > 0 {
		log.Println("User Gru was inserted")
	}

	log.Println("Rolling back transaction...")
	check(tx.Rollback())

	row = db.QueryRow("SELECT COUNT(*) FROM users WHERE name = ?", "gru")
	check(row.Scan(&count))
	if count > 0 {
		log.Println("Found user Gru")
	} else {
		log.Println("Didn't find user Gru")
	}
}

func testPreparedStmt() {
	stmt, err := db.Prepare("INSERT INTO users VALUES(?, ?, ?, ?, ?)")
	check(err)
	defer stmt.Close()

	check(stmt.Exec("Kevin", 11, 0.55, true, "2013-07-06"))
	check(stmt.Exec("Bob", 12, 0.73, true, "2012-11-04"))
	check(stmt.Exec("Stuart", 13, 0.66, true, "2014-02-12"))

	stmt, err = db.Prepare("SELECT * FROM users WHERE age > ?")
	check(err)

	rows, err := stmt.Query(1)
	check(err)
	defer rows.Close()

	for rows.Next() {
		u := new(user)
		err := rows.Scan(&u.name, &u.age, &u.height, &u.awesome, &u.bday)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf(
			"%s is %d years old, %.2f tall, bday on %s and has awesomeness: %t\n",
			u.name, u.age, u.height, u.bday.Format(time.RFC3339), u.awesome,
		)
	}
}

type purchase struct {
	userId    int
	total     float32
	timestamp time.Time
}

func testPurchase() {
	check(db.Exec("CREATE TABLE purchases(user_id INTEGER, total DOUBLE, timestamp TIMESTAMP)"))
	check(db.Exec("INSERT INTO purchases VALUES(31, 9.9, '2021-09-20 11:39:00')"))
	check(db.Exec("INSERT INTO purchases VALUES(31, 19.2, '2021-09-20 11:39:00')"))
	check(db.Exec("INSERT INTO purchases VALUES(99, 23.4, '2021-10-20 09:12:00')"))
	check(db.Exec("INSERT INTO purchases VALUES(99, 11.4, '2021-08-20 09:12:00')"))
	check(db.Exec("INSERT INTO purchases VALUES(44, 19.2, '2021-11-20 08:19:00')"))
	check(db.Exec("INSERT INTO purchases VALUES(45, 22.9, '2021-12-20 19:23:00')"))
	check(db.Exec("INSERT INTO purchases VALUES(45, 23.4, '2021-11-20 19:23:00')"))

	now := time.Now().UTC()
	t, _ := time.Parse(time.RFC3339, "2021-03-11T15:04:05+07:00")

	stmt, err := db.Prepare("INSERT INTO purchases VALUES(?, ?, ?)")
	check(err)
	defer stmt.Close()

	check(stmt.Exec(31, float32(12.8), now))
	check(stmt.Exec(77, float32(12.8), t))

	rows, err := db.Query(`
		SELECT user_id, total, timestamp
		FROM purchases 
		WHERE total > ?`,
		float32(10.0),
	)
	check(err)
	defer rows.Close()

	for rows.Next() {
		p := new(purchase)
		err := rows.Scan(&p.userId, &p.total, &p.timestamp)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf(
			"%d user purchased %f at %v", p.userId, p.total, p.timestamp,
		)
	}
	check(rows.Err())
}

type purchaseAggregate struct {
	userId        int
	average_total float32
}

func openWithReadOnly() {
	rdb, err := sql.Open("duckdb", "duckdb.db?access_mode=READ_ONLY")
	if err != nil {
		log.Fatal(err)
	}
	defer rdb.Close()

	check(rdb.Ping())

	rows, err := rdb.Query(
		`SELECT user_id, avg(total) as average_total FROM purchases WHERE year(timestamp) = ? GROUP BY user_id`,
		2021,
	)
	check(err)
	defer rows.Close()
	for rows.Next() {
		p := new(purchaseAggregate)
		err := rows.Scan(&p.userId, &p.average_total)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf(
			"average purchase %d is %f", p.userId, p.average_total,
		)
	}

	if _, err = rdb.Exec("INSERT INTO purchases VALUES(31, 9.9, '2021-09-20 11:39:00')"); err != nil {
		log.Printf(
			"write query expected to fail with read only connection: %v", err,
		)
	}
}
