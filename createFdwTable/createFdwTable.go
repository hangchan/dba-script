package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
	"regexp"
	"strings"
)

var (
	username string = os.Getenv("fdwUsername")
	password string = os.Getenv("fdwPassword")
	dbHost   string = os.Getenv("fdwDbHost")
	database string = os.Args[1]
	table string =  os.Args[2]
	shortHost = strings.Split(dbHost, ".")
	shortDatabase = strings.Split(database, "_")
)

func main() {
	results := runQuery(table)

	fmt.Printf("CREATE FOREIGN TABLE IF NOT EXISTS %s_fdw.%s (\n", database, table)
	colOutput := printFTable(results)
	for i, v := range colOutput {
		if i < len(colOutput) - 1 {
			fmt.Printf("%s,\n", v)
		} else {
			fmt.Printf(v)
		} 
	}
	fmt.Println("\n)")
	fmt.Printf("SERVER mysql_server_%s_%s\n", shortHost[0], shortDatabase[1])
	fmt.Printf("OPTIONS (dbname '%s', table_name '%s');\n", database, table)
}

func dbConn() string {
	dbConn := strings.Join([]string{username, ":", password, "@(", dbHost, ":3306)/", database, "?interpolateParams=true"}, "")

	return dbConn
}

func runQuery(table string) []string {
	db, err := sql.Open("mysql", dbConn())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var results string
	var results2 string
	sql := strings.Join([]string{"SHOW CREATE TABLE ", table}, "")

	err = db.QueryRow(sql).Scan(&results, &results2)
	if err != nil {
		log.Fatal(err)
	}
	resultsArr := strings.Split(results2, "\n")
	return resultsArr
}

func printFTable(results []string) []string {
	var colOutput []string
	for _, v := range results {
		reStr := regexp.MustCompile(`^\s\s.(.*?).\s(.*?)\s.*,$`)
		reInt := regexp.MustCompile(`^(.*?)\s(bigint|tinyint|int)\((\d+)\)$`)
		reVar := regexp.MustCompile(`^(.*?)\s(char|varchar)\((\d+)\)$`)
		reTime := regexp.MustCompile(`^(.*?)\s(datetime|timestamp)$`)
		reEnum := regexp.MustCompile(`^(.*?)\s(enum.*?)$`)
		if reStr.MatchString(v) {
			//fmt.Printf("%d: ", i)
			repStr := "$1 $2"
			output := reStr.ReplaceAllString(v, repStr)
			output = strings.ToLower(output)
			//fmt.Println(output)
			switch {
			case reInt.MatchString(output):
				repInt := "$1 int"
				intOutput := reInt.ReplaceAllString(output, repInt)
				colOutput = append(colOutput, fmt.Sprintf("%s", intOutput))
			case reVar.MatchString(output):
				repVar := "$1 varchar($3)"
				varOutput := reVar.ReplaceAllString(output, repVar)
				colOutput = append(colOutput, fmt.Sprintf("%s", varOutput))
			case reTime.MatchString(output):
				repTime := "$1 timestamp"
				timeOutput := reTime.ReplaceAllString(output, repTime)
				colOutput = append(colOutput, fmt.Sprintf("%s with time zone", timeOutput))
			case reEnum.MatchString(output):
				repEnum := "$1 varchar(32)"
				enumOutput := reEnum.ReplaceAllString(output, repEnum)
				colOutput = append(colOutput, fmt.Sprintf("%s", enumOutput))
			}
		}
	}

	return colOutput
}
