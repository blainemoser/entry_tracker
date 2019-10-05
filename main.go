package main

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Checks whether a fatal error has been encountered
func checkFatalErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
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

	return result
}

func main() {
	// Find configs in the provided config file
	// configData, err := ioutil.ReadFile("config.txt")
	// checkFatalErr(err)

	configs := getConfigs()
	fmt.Println(configs)

	// Run an indefinite update loop
	for now := range time.Tick(time.Minute) {
		fmt.Println(now, "tickin.....")
	}
}
