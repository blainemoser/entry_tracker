package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/blainemoser/jsonextract"

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

// Checks whether a non-fatal error has been encountered
func checkNonFatalErr(err error) bool {
	if err != nil {
		log.Println(err.Error())
		return true
	}
	return false
}

func checkConfigs(configs configs) {
	var expects = map[string]string{
		"username": "username",
		"password": "password",
		"port":     "port",
		"driver":   "driver",
		"host":     "host",
		"database": "database",
	}
	expectsCount := len(expects)
	countParams := 0
	for key, _ := range configs.databaseConfigs {
		if key == "username" ||
			key == "password" ||
			key == "port" ||
			key == "driver" ||
			key == "host" ||
			key == "database" {
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
	properties map[string]interface{}
	database   string
	table      string
	connection *sql.DB
}

func (r *record) save() {

	insertStatement := "INSERT INTO `" + r.database + "`.`" + r.table + "` (`@fields`) VALUES (@values)"

	var inserts []interface{}
	var fields []string
	var valuesEscapes []string

	for field, value := range r.properties {
		fields = append(fields, field)
		valuesEscapes = append(valuesEscapes, "?")
		inserts = append(inserts, value)
	}

	insertStatement = strings.Replace(insertStatement, "@fields", strings.Join(fields, "`, `"), 1)
	insertStatement = strings.Replace(insertStatement, "@values", strings.Join(valuesEscapes, ", "), 1)
	insert, err := r.connection.Exec(insertStatement, inserts[:]...)
	// handle any error with the insert
	if err != nil {
		panic("Error in `func (r *record) save()` insert: " + err.Error())
	}
	lastInsertID, err := insert.LastInsertId()
	if err != nil {
		panic(err.Error())
	}
	fmt.Printf("Created Record No. %d\n", lastInsertID)
	return
}

func jsonDecode(data string) (interface{}, error) {
	var dat map[string]interface{}
	err := json.Unmarshal([]byte(data), &dat)
	return dat, err
}

func getFileContents(fileName string) (string, error) {
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0700)
	fileUnmarshalled := make([]byte, 1024)
	fileRead, err := file.Read(fileUnmarshalled)
	if err != nil {
		return "", err
	}
	fileContents := string(fileUnmarshalled[:fileRead])

	// Close the file
	if err := file.Close(); err != nil {
		log.Fatal(err)
	}

	return fileContents, nil
}

func getConfigs(fileName string) configs {

	// Get the configs from the file
	fileContents, err := getFileContents(fileName)
	checkFatalErr(err)

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

		if strings.ToLower(strings.TrimSpace(keyValPair[0])) == "file" {
			result.urls[countUrls] = strings.TrimSpace(keyValPair[1])
			countUrls++
		} else {
			result.databaseConfigs[strings.TrimSpace(keyValPair[0])] = strings.TrimSpace(keyValPair[1])
		}
	}

	checkConfigs(result)

	return result
}

func getTime(args interface{}) time.Duration {
	// default
	frequency := time.Hour
	if args, ok := args.([]string); ok {
		selection := strings.ToLower(args[0])
		if len(args) > 0 {
			switch selection {
			case "minute":
				frequency := time.Minute
				return frequency
			case "quarter-hour":
				frequency := time.Minute * 15
				return frequency
			case "half-hour":
				frequency := time.Minute * 30
				return frequency
			case "three-quarter-hour":
				frequency := time.Minute * 45
				return frequency
			case "hour":
				frequency := time.Hour
				return frequency
			case "day":
				frequency := time.Hour * 24
				return frequency
			default:
				frequency, err := time.ParseDuration(selection)
				if err != nil {
					log.Printf("Could not parse the provided time interval: %s. Setting inteval to one hour.\n", err.Error())
					return time.Hour
				}
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

func openDb(dbConfigs map[string]string) *sql.DB {
	// connect to database
	db, err := sql.Open(dbConfigs["driver"], dbConfigs["username"]+":"+dbConfigs["password"]+"@tcp("+dbConfigs["host"]+":"+dbConfigs["port"]+")/"+dbConfigs["database"])
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func getMappingProperties(prop string) (map[string]string, error) {

	// find the file:
	mapping, err := getFileContents(prop)
	if err != nil {
		return nil, err
	}

	mappingJSON, err := jsonDecode(mapping)
	if err != nil {
		return nil, err
	}

	result := make(map[string]string)
	if mappingJSON, ok := mappingJSON.(map[string]interface{}); ok {
		for i, v := range mappingJSON {
			if v, ok := v.(string); ok {
				result[i] = v
			} else {
				return nil, errors.New("mapping json is not parseable")
			}
		}
	} else {
		return nil, errors.New("mapping json is not parseable")
	}

	return result, nil
}

func fetchAndSave(nextURL string, database string, db *sql.DB) {
	mappingProperties, err := getMappingProperties(nextURL)
	if nferr := checkNonFatalErr(err); nferr {
		return
	}

	// Expects the properties url and table
	url := mappingProperties["url"]
	table := mappingProperties["table"]

	if url == "" {
		fmt.Println("mapping is missing url property")
		return
	}

	if table == "" {
		fmt.Println("mapping is missing table property")
		return
	}

	delete(mappingProperties, "url")
	delete(mappingProperties, "table")

	details := getJSONPayload(url)
	payload := &jsonextract.JSONExtract{RawJSON: details}

	// for the remaining properties, get the payload
	properties := make(map[string]interface{})
	for field, val := range mappingProperties {
		prop, err := payload.Extract(val)
		if nferr := checkNonFatalErr(err); nferr {
			return
		}
		properties[field] = prop
	}

	// Create a new record object and save same
	r := &record{properties, database, table, db}
	r.save()
	return
}

func main() {
	// Find configs in the provided config file
	args := os.Args[1:]
	if len(args) < 1 {
		checkFatalErr(errors.New("no interval provided"))
	}

	// Run an indefinite update loop
	for now := range time.Tick(getTime(args)) {

		configs := getConfigs("config.txt")
		dbConfigs := configs.databaseConfigs
		urls := configs.urls
		db := openDb(dbConfigs)
		defer db.Close()

		// Each tick, connect to the URLS specified and save the records to the database
		fmt.Println(now)
		for _, url := range urls {
			wg.Add(1)
			go func(nextURL string, database string, db *sql.DB) {
				defer catchWgAndPanic()
				fetchAndSave(nextURL, database, db)
				return
			}(url, dbConfigs["database"], db)
		}
		wg.Wait()
	}
}
