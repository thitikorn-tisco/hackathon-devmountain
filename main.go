package main

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var DB *gorm.DB

type Records struct {
	XMLName xml.Name `xml:"records"`
	Records []Record `xml:"record"`
}

type Record struct {
	XMLName     xml.Name `xml:"record"`
	EmpId       string   `xml:"EMPID"`
	Passport    string   `xml:"PASSPORT"`
	Firstname   string   `xml:"FIRSTNAME"`
	Lastname    string   `xml:"LASTNAME"`
	Gender      string   `xml:"GENDER"`
	Birthday    string   `xml:"BIRTHDAY"`
	Nationality string   `xml:"NATIONALITY"`
	Hired       string   `xml:"HIRED"`
	Dept        string   `xml:"DEPT"`
	Position    string   `xml:"POSITION"`
	Status      string   `xml:"STATUS"`
	Region      string   `xml:"REGION"`
}

type DevMountain struct {
	EmpId       string
	Passport    string
	Firstname   string
	Lastname    string
	Gender      string
	Birthday    string
	Nationality string
	Hired       string
	Dept        string
	Position    string
	Status      string
	Region      string
}

type DevClub struct {
	EmpId       string
	Passport    string
	Firstname   string
	Lastname    string
	Gender      string
	Birthday    string
	Nationality string
	Hired       string
	Dept        string
	Position    string
	Status      string
	Region      string
}

// 1 = Active
// 2 = Resigned
// 3 = Retired
var mapStatus = map[string]string{"1": "Active", "2": "Resigned", "3": "Retired"}

// 0 = Male
// 1 = Female
var mapGender = map[string]string{"0": "Male", "1": "Female"}

func connectSQLLite() {
	db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}

	db.Exec("drop table dev_mountains;drop table dev_clubs")
	db.AutoMigrate(
		&DevMountain{},
		&DevClub{},
	)

	DB = db
}

func unmarshalXml() Records {
	xmlFile, err := os.Open("./data-devclub-1.xml")
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("Read File Success")
	defer xmlFile.Close()

	byteValue, _ := ioutil.ReadAll(xmlFile)

	var records Records
	xml.Unmarshal(byteValue, &records)
	return records
}

func validateGenderAndStatus(empInfo DevMountain) bool {
	_, foundGender := mapGender[empInfo.Gender]
	_, foundStatus := mapStatus[empInfo.Status]
	return foundGender && foundStatus
}
func migrateData(records Records) {
	for i := 0; i < len(records.Records); i++ {
		empInfo := DevMountain{
			EmpId:       records.Records[i].EmpId,
			Passport:    records.Records[i].Passport,
			Firstname:   records.Records[i].Firstname,
			Lastname:    records.Records[i].Lastname,
			Gender:      records.Records[i].Gender,
			Birthday:    records.Records[i].Birthday,
			Nationality: records.Records[i].Nationality,
			Hired:       records.Records[i].Hired,
			Dept:        records.Records[i].Dept,
			Position:    records.Records[i].Position,
			Status:      records.Records[i].Status,
			Region:      records.Records[i].Region,
		}
		if validateGenderAndStatus(empInfo) {
			DB.Create(&empInfo)
		}
	}

	var resultPositions []DevMountain
	DB.Raw("select * from dev_mountains where position in ('Pilot', 'Airhostess', 'Steward')").Scan(&resultPositions)
	fmt.Println("resultPosition=>", len(resultPositions))

	for _, resultPosition := range resultPositions {
		hiredDate := getDate(resultPosition.Hired)
		back3Year := time.Now().AddDate(-3, 0, 0)

		var devClub DevClub
		devClub.EmpId = resultPosition.EmpId
		devClub.Passport = resultPosition.Passport
		devClub.Firstname = resultPosition.Firstname
		devClub.Lastname = resultPosition.Lastname
		devClub.Gender = resultPosition.Gender
		devClub.Birthday = resultPosition.Birthday
		devClub.Nationality = resultPosition.Nationality
		devClub.Hired = resultPosition.Hired
		devClub.Dept = resultPosition.Dept
		devClub.Position = resultPosition.Position
		devClub.Status = resultPosition.Status
		devClub.Region = resultPosition.Region

		// fmt.Println(idx + 1)
		// fmt.Println(hiredDate.Before(back3Year))

		if hiredDate.Before(back3Year) && devClub.Status == "1" {
			DB.Create(&devClub)
		}
	}
}
func getDate(dateStr string) time.Time {
	date, err := strconv.Atoi(dateStr[0:2])
	if err != nil {
		fmt.Println(err)
	}
	month, err := strconv.Atoi(dateStr[3:5])
	if err != nil {
		fmt.Println(err)
	}
	year, err := strconv.Atoi(dateStr[6:10])
	if err != nil {
		fmt.Println(err)
	}
	// fmt.Printf("Date substr->: %s%s%s\n", strconv.Itoa(date), strconv.Itoa(month), strconv.Itoa(year))

	return time.Date(year, time.Month(month), date, 0, 0, 0, 0, time.Local)
}

func createCSVFile() {
	// Create CSV file
	var distinctNationalities []string
	DB.Raw("select DISTINCT(nationality) from dev_clubs").Scan(&distinctNationalities)
	fmt.Println(len(distinctNationalities))
	for _, distintNationality := range distinctNationalities {
		// fmt.Println(distintNationality)
		f, err := os.Create("./tmp/" + "employee_in_" + distintNationality)
		if err != nil {
			fmt.Println(err)
		}
		defer f.Close()

		var resultByNationalities []DevClub
		DB.Raw("select * from dev_clubs where nationality = ?", distintNationality).Scan(&resultByNationalities)
		var strcsv string = "EMPID,PASSPORT,FIRSTNAME,LASTNAME,GENDER,BIRTHDAY,NATIONALITY,HIRED,DEPT,POSITION,STATUS,REGION\n"
		for _, resultByNationality := range resultByNationalities {
			strcsv = strcsv + fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", resultByNationality.EmpId, resultByNationality.Passport, resultByNationality.Firstname, resultByNationality.Lastname, resultByNationality.Gender, resultByNationality.Birthday, resultByNationality.Nationality, resultByNationality.Hired, resultByNationality.Dept, resultByNationality.Position, resultByNationality.Status, resultByNationality.Region)
		}
		f.WriteString(strcsv)
	}
}
func main() {
	connectSQLLite()

	records := unmarshalXml()

	migrateData(records)

	createCSVFile()
}
