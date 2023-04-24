package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	databaseURL = "postgres://user:admin@localhost:5432/promotions_db?sslmode=disable"
	period      = 30
)

type Promotion struct {
	ID             string
	Price          float64
	ExpirationDate time.Time
}

func main() {

	// Connect to the database
	log.Println("Connecting to DB...")
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		log.Fatal("error connecting to the database: ", err)
	}
	defer db.Close()

	// Create a table to store the records if it does not exist
	log.Println("Creating table if not exists...")
	if _, err := db.Exec("CREATE TABLE IF NOT EXISTS promotions (id TEXT PRIMARY KEY, price FLOAT, expiration_date TIMESTAMP)"); err != nil {
		log.Fatal("error creating table: ", err)
	}

	// Read CSV file
	file := openCSV(err)

	promotions, err := readPromotionsFromCSV(file.Name())

	if err != nil {
		log.Fatalf("error reading promotions from CSV:%v", err)
	}

	// Clear the existing records from the database
	log.Println("Truncating table before inserting new data...")
	if _, err := db.Exec("TRUNCATE TABLE promotions"); err != nil {
		log.Fatalf("error in db:%v", err)
	}

	// Insert promotions to database in batches
	log.Println("Starting to insert CSV file data to DB")

	batchSize := 1000
	for i := 0; i < len(promotions); i += batchSize {
		end := i + batchSize
		if end > len(promotions) {
			end = len(promotions)
		}
		err := insertPromotions(db, promotions[i:end])
		if err != nil {
			log.Fatal("error inserting promotions to database:", err)
		}
	}
	log.Println("Data inserted!")

	// Clear the existing records from the database
	//go truncatePromotionsTable(db, period*time.Minute)

	go func() {
		time.Sleep(30 * time.Second)
		for {
			insertNewDataEachN_Min(db, err)
			if err != nil {
				log.Printf("error truncating promotions table: %v", err)
			}
			time.Sleep(period * time.Minute)
		}
	}()

	// Start the HTTP server
	// Expose an HTTP endpoint to handle incoming requests
	// Call handleRequests function
	handleRequests(db)

}

func openCSV(err error) *os.File {
	log.Println("Starting to read CSV file...")
	file, err := os.Open("promotions.csv")
	if err != nil {
		log.Fatal("error opening file:", err)
	}
	defer file.Close()
	return file
}

func insertPromotions(db *sql.DB, promotions []Promotion) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
			return
		}
		err = tx.Commit()
	}()

	stmt, err := tx.Prepare("INSERT INTO promotions(id, price, expiration_date) VALUES ($1, $2, $3)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, p := range promotions {
		_, err = stmt.Exec(p.ID, p.Price, p.ExpirationDate)
		if err != nil {
			return err
		}
	}
	return nil
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	_, err := fmt.Fprint(w, "Promotions Api")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func insertNewDataEachN_Min(db *sql.DB, err error) {
	_, err = db.Exec("TRUNCATE TABLE promotions")
	if err != nil {
		return
	}
	updatedFile := openCSV(err)
	promotionsFromCSV, err := readPromotionsFromCSV(updatedFile.Name())
	if err != nil {
		return
	}
	err = insertPromotions(db, promotionsFromCSV)
	if err != nil {
		log.Printf("error inserting promotions: %v", err)
	}
	log.Println("New CSV file inserted")

}
func readPromotionsFromCSV(filename string) ([]Promotion, error) {
	var promotions []Promotion

	file, err := os.Open(filename)
	if err != nil {
		return promotions, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// Set the delimiter to a comma
	reader.Comma = ','
	// Skip the header row
	reader.Read()

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return promotions, err
		}

		id := record[0]

		// Multiple date formats for parse the expiration date
		var expirationDate time.Time
		for _, dateFormat := range []string{"2006-01-02 15:04:05 -0700 MST", time.RFC3339} {
			expirationDate, err = time.Parse(dateFormat, record[2])
			if err == nil {
				break
			}
		}
		if err != nil {
			return promotions, fmt.Errorf("error parsing expiration date: %v", err)
		}

		price, err := strconv.ParseFloat(record[1], 64)
		if err != nil {
			return promotions, fmt.Errorf("error parsing price: %v", err)
		}

		if err != nil {
			return promotions, fmt.Errorf("error parsing price: %v", err)
		}
		promotion := Promotion{
			ID:             id,
			Price:          price,
			ExpirationDate: expirationDate,
		}
		promotions = append(promotions, promotion)
	}

	return promotions, nil
}

func handleRequests(db *sql.DB) {
	http.HandleFunc("/", indexHandler)
	port := os.Getenv("PORT")
	if port == "" {
		port = "1321"
		log.Printf("Default port %s", port)
	}

	log.Printf("Open http://localhost:%s in the browser", port)
	http.HandleFunc("/promotions/", func(w http.ResponseWriter, r *http.Request) {

		// Parse the ID from the URL
		parts := strings.Split(r.URL.Path, "/")
		id := parts[len(parts)-1]
		log.Printf("ID from request %v", id)
		// If the ID is empty, return an error
		if id == "" {
			http.Error(w, "Missing promotion ID", http.StatusBadRequest)
			return
		}

		// Call the getPromotionByID function to retrieve the promotion
		promotion, err := getPromotionByID(db, id)
		if err != nil {
			if err == sql.ErrNoRows {
				http.Error(w, "Promotion not found", http.StatusNotFound)
			} else {
				http.Error(w, "Error retrieving promotion", http.StatusInternalServerError)
			}
			return
		}

		// Serialize the promotion to JSON and write it to the response
		response, err := json.Marshal(promotion)
		if err != nil {
			http.Error(w, "Error serializing promotion", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(response)
	})

	log.Fatal(http.ListenAndServe(":1321", nil))
}

// retrieves a promotion from the database by ID
func getPromotionByID(db *sql.DB, id string) (*Promotion, error) {
	var promotion Promotion
	err := db.QueryRow("SELECT id, price, expiration_date FROM promotions WHERE id = $1", id).Scan(
		&promotion.ID,
		&promotion.Price,
		&promotion.ExpirationDate,
	)

	if err == sql.ErrNoRows {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("error querying the database: %w", err)
	}
	return &promotion, nil
}
