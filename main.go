package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var wg sync.WaitGroup

// Debugging function
func dd(v interface{}) {
	fmt.Println(v)
	os.Exit(0)
}

func catchWgAndPanic() {
	if r := recover(); r != nil {
		fmt.Println("recovered from panic: ", r)
	}
	wg.Done()
}

// Checks whether a fatal error has been encountered
func checkFatalErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func checkConfigs(configs configs) {
	var expects = map[string]string{
		"username": "username",
		"password": "password",
		"port":     "port",
		"driver":   "driver",
		"host":     "host",
		"database": "database",
		"table":    "table",
	}
	expectsCount := len(expects)
	countParams := 0
	for key, _ := range configs.databaseConfigs {
		if key == "username" ||
			key == "password" ||
			key == "port" ||
			key == "driver" ||
			key == "host" ||
			key == "database" ||
			key == "table" {
			delete(expects, key)
			countParams++
		}
	}

	if countParams != expectsCount {
		// Report on the missing configs
		missing := ""
		for _, j := range expects {
			if missing != "" {
				missing += "; " + j
			} else {
				missing += j
			}
		}
		panic("Missing database configs: " + missing)
	}
	return
}

type configs struct {
	urls            map[int]string
	databaseConfigs map[string]string
}

type record struct {
	payload  string
	url      string
	database string
	table    string
}

func (r *record) save(db *sql.DB) {
	insert, err := db.Exec("INSERT INTO `"+r.database+"`.`"+r.table+"` (`url`, `payload`) VALUES (?, ?)", r.url, r.payload)
	// if there is an error inserting, handle it
	if err != nil {
		panic("Error in `func (r *record) save(db *sql.DB)` insert: " + err.Error())
	}
	fmt.Println(insert)
	return
}

func getConfigs() configs {
	configFile, err := os.OpenFile("config.txt", os.O_RDONLY, 0700)
	checkFatalErr(err)

	// Get the configs from the file
	configFileUnmarshalled := make([]byte, 1024)
	configFileRead, err := configFile.Read(configFileUnmarshalled)
	checkFatalErr(err)

	fileContents := string(configFileUnmarshalled[:configFileRead])

	splitter := regexp.MustCompile(`\n`) // splits the file contents by each new line
	lines := splitter.Split(fileContents, -1)
	var result configs
	result.urls = make(map[int]string)
	result.databaseConfigs = make(map[string]string)
	countUrls := 0 // there can be multiple urls
	for _, line := range lines {
		configSplitter := regexp.MustCompile(`:`) // looks for colon to denote config value

		commentIndex := strings.Index(line, "#") // comments use "#" indicators in config files
		if commentIndex < 0 {
			commentIndex = len(line)
		}
		config := strings.TrimSpace(line[0:commentIndex])
		keyValPair := configSplitter.Split(config, 2)
		if len(keyValPair) != 2 {
			continue
		}

		if strings.ToLower(strings.TrimSpace(keyValPair[0])) == "url" {
			result.urls[countUrls] = strings.TrimSpace(keyValPair[1])
			countUrls++
		} else {
			result.databaseConfigs[strings.TrimSpace(keyValPair[0])] = strings.TrimSpace(keyValPair[1])
		}
	}
	// Close the file
	if err := configFile.Close(); err != nil {
		log.Fatal(err)
	}

	checkConfigs(result)

	return result
}

func getTime(args interface{}) time.Duration {
	// default
	frequency := time.Minute
	if args, ok := args.([]string); ok {
		if len(args) > 0 {
			switch strings.ToLower(args[0]) {
			case "hour":
				frequency := time.Hour
				return frequency
			case "day":
				frequency := time.Hour * 24
				return frequency
			case "half-hour":
				frequency := time.Minute * 30
				return frequency
			case "quarter-hour":
				frequency := time.Minute * 15
				return frequency
			case "three-quarter-hour":
				frequency := time.Minute * 45
				return frequency
			case "minute":
				frequency := time.Minute
				return frequency
			default:
				frequency := time.Minute
				return frequency
			}
		}
	}
	return frequency
}

func getJSONPayload(url string) string {
	resp, err := http.Get(url)
	if err != nil {
		panic(err.Error())
	}
	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err.Error())
	}

	return string(bytes)
}

func main() {
	// Find configs in the provided config file
	args := os.Args[1:]

	// This accepts only one argument - the frequency of the scrubbing
	configs := getConfigs()
	dbConfigs := configs.databaseConfigs
	urls := configs.urls

	// Run an indefinite update loop
	for now := range time.Tick(getTime(args)) {

		// connect to database
		db, err := sql.Open(dbConfigs["driver"], dbConfigs["username"]+":"+dbConfigs["password"]+"@tcp("+dbConfigs["host"]+":"+dbConfigs["port"]+")/"+dbConfigs["database"])
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		// Each tick, connect to the URLS specified and save the records to the database
		fmt.Println(now)
		for _, url := range urls {
			wg.Add(1)
			go func(nextUrl string, database string, table string, db *sql.DB) {
				defer catchWgAndPanic()
				details := getJSONPayload(nextUrl)
				r := &record{details, nextUrl, database, table}
				r.save(db)
				return
			}(url, dbConfigs["database"], dbConfigs["table"], db)
		}
		wg.Wait()
	}
}
