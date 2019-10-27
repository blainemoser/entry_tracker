package main

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "github.com/blainemoser/jsonextract"
)

func TestGetConfigs(t *testing.T) {
	result := getConfigs("tests/config_test.txt")
	dbConfigsExpectations := map[string]string{
		"database": "dbase",
		"host":     "localhost",
		"username": "uname",
		"password": "pword",
		"port":     "3306",
		"driver":   "mysql",
	}

	urlsExpectations := map[int]string{
		0: "tests/test_mapping_1.json",
		1: "tests/test_mapping_2.json",
	}

	for key, value := range dbConfigsExpectations {
		if result.databaseConfigs[key] != value {
			t.Errorf("Incorrect database config for %s; got: %s, want: %s", key, result.databaseConfigs[key], value)
		}
	}

	for key, value := range urlsExpectations {
		if result.urls[key] != value {
			t.Errorf("Incorrect URL; got: %s, want: %s", result.urls[key], value)
		}
	}

	return
}

func TestGetTime(t *testing.T) {
	custom, _ := time.ParseDuration("1m32s")
	times := map[string]time.Duration{
		"three-quarter-hour": time.Minute * 45,
		"1m32s":              custom,
		"errant":             time.Hour,
	}
	for key, time := range times {
		input := []string{key}
		frequency := getTime(input)
		if frequency != time {
			t.Errorf("`getTime` responded with an incorrect duration for the input %s; got: %s, want: %s", key, frequency.String(), time.String())
		}

	}
}

func TestJsonMappingRetrieval(t *testing.T) {
	mappingProperties, err := getMappingProperties("tests/test_mapping_1.json")
	if err != nil {
		t.Errorf("unexpected error in `TestJsonMappingRetrieval`: %s", err.Error())
	}

	if err != nil {
		t.Errorf("unexpected error in `TestJsonMappingRetrieval`: %s", err.Error())
	}

	if mappingProperties["url"] != "https://blockchain.info/ticker" {
		t.Errorf("`getMappingProperties` responded with an unexpected result for url; got: `%s`, want: `https://blockchain.info/ticker`", mappingProperties["url"])
	}

	if mappingProperties["table"] != "temp_testing_table" {
		t.Errorf("`getMappingProperties` responded with an unexpected result for table; got: `%s`, want: `temp_testing_table`", mappingProperties["table"])
	}

	if mappingProperties["test_property"] != "USD/buy" {
		t.Errorf("`getMappingProperties` responded with an unexpected result for the test property; got: `%s`, want: `USD/buy`", mappingProperties["test_property"])
	}

	return
}

// Note that the config file needs to be completed for this test
func TestDatabaseConnection(t *testing.T) {
	configs := getConfigs("config.txt")
	dbConfigs := configs.databaseConfigs
	db := openDb(dbConfigs)
	db.Close()
}

func dropTempTable(db *sql.DB) error {
	tempTableExistsThenDrop := "DROP TABLE IF EXISTS `temp_testing_table`;"
	_, err := db.Exec(tempTableExistsThenDrop)
	if err != nil {
		return err
	}
	return nil
}

func createTempTable(db *sql.DB) error {
	// create a temporary table
	tempTable := "CREATE TABLE `temp_testing_table`(`id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,`test_property` VARCHAR(50), PRIMARY KEY (`id`));"
	_, err := db.Exec(tempTable)
	if err != nil {
		return err
	}
	return nil
}

func TestSaving(t *testing.T) {

	configs := getConfigs("config.txt")
	dbConfigs := configs.databaseConfigs
	db := openDb(dbConfigs)
	err := dropTempTable(db)
	err = createTempTable(db)
	if err != nil {
		t.Errorf("unexpected error while checking for/creating `temp_testing_table` in `createTempTable`: %s", err.Error())
	}

	properties := make(map[string]interface{})
	properties["test_property"] = "test_value"
	r := &record{properties, dbConfigs["database"], "temp_testing_table", db}
	r.save()

	// No panics up until this point
	err = dropTempTable(db)
	if err != nil {
		t.Errorf("unexpected error while dropping table `temp_testing_table` in `TestSaving`: %s", err.Error())
	}
	db.Close()
}

func TestGetJSONPayload(t *testing.T) {
	// Using an aribitrary open api here
	result := getJSONPayload("https://api.coindesk.com/v1/bpi/currentprice/USD.json") // ! should make this more robust since this relies on an external open API
	if strings.Index(result, "\"bpi\":") == -1 {
		t.Errorf("`getJSONPayload` return unexpected result; got: %s, want JSON to contain: \"bpi\":", result)
	}
	return
}

// Runs general test of concept
func TestGeneral(t *testing.T) {
	configs := getConfigs("config.txt")
	dbConfigs := configs.databaseConfigs
	testConfigs := getConfigs("tests/config_test.txt")
	urls := testConfigs.urls
	db := openDb(dbConfigs)
	defer db.Close()

	err := dropTempTable(db)
	err = createTempTable(db)
	if err != nil {
		t.Errorf("unexpected error while checking for/creating `temp_testing_table` in `createTempTable`: %s", err.Error())
	}

	for _, url := range urls {
		wg.Add(1)
		go func(nextURL string, database string, db *sql.DB) {
			defer wg.Done()
			fetchAndSave(nextURL, database, db)
			return
		}(url, dbConfigs["database"], db)
	}

	wg.Wait()

	err = dropTempTable(db)
	if err != nil {
		t.Errorf("unexpected error while dropping table `temp_testing_table` in `TestSaving`: %s", err.Error())
	}
}

// func TestRecordSave(t *testing.T) {
// 	r := record
// }
