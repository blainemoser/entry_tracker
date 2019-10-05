package main

import (
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// Find configs in the provided config file
	configFile, err := os.OpenFile("confg.txt", os.O_RDONLY, 0700)
	if err != nil {
		log.Fatal(err)
	}

	// Get the configs from the file

	// Close the file
	if closeErr := configFile.Close(); closeErr != nil {
		log.Fatal(closeErr)
	}

	// Run an indefinite update loop
	for now := range time.Tick(time.Minute) {
		fmt.Println(now, "tickin.....")
	}
}
