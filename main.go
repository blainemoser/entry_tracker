package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Debugging function
func dd(v interface{}) {
	fmt.Println(v)
	os.Exit(0)
}

// Checks whether a fatal error has been encountered
func checkFatalErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func checkConfigs(configs configs) bool {
	var expects = map[string]string{"username": "username", "password": "password", "port": "port", "driver": "driver", "host": "host", "database": "database"}
	expectsCount := len(expects)
	countParams := 0
	for key, _ := range configs.databaseConfigs {
		if key == "username" || key == "password" || key == "port" || key == "driver" || key == "host" || key == "database" {
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
	return true
}

type configs struct {
	urls            map[int]string
	databaseConfigs map[string]string
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

func main() {
	// Find configs in the provided config file
	// configData, err := ioutil.ReadFile("config.txt")
	// checkFatalErr(err)

	configs := getConfigs()
	dbConfigs := configs.databaseConfigs
	// urls := configs.urls

	// connect to database
	db, err := sql.Open(dbConfigs["driver"], dbConfigs["username"]+":"+dbConfigs["password"]+"@tcp("+dbConfigs["host"]+":"+dbConfigs["port"]+")/"+dbConfigs["database"])
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Run an indefinite update loop
	for now := range time.Tick(time.Minute) {
		fmt.Println(now, "tickin.....")
	}
}
