package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"
	//"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// SchemaValue is an entry of the map Config
type SchemaValue struct {
	Name   string
	Length int
	Start  int
	End    int
}

// Config is the map
type Config map[string][]SchemaValue

// UnmarshalJSON parse the json file and populate Config map
func (t *Config) UnmarshalJSON(b []byte) error {

	// Create a local struct that mirrors the data being unmarshalled
	type schemaEntry struct {
		Message string `json:"message"`
		Name    string `json:"name"`
		Length  int    `json:"len"`
		Start   int    `json:"start"`
		End     int    `json:"end"`
	}

	var entries []schemaEntry

	// unmarshal the data into the slice
	if err := json.Unmarshal(b, &entries); err != nil {
		return err
	}

	tmp := make(Config)

	// loop over the slice and create the map of entries
	for _, ent := range entries {
		tmp[ent.Message] = append(tmp[ent.Message], SchemaValue{Name: ent.Name, Length: ent.Length, Start: ent.Start, End: ent.End})
	}

	// assign the tmp map to the type
	*t = tmp
	return nil
}

// Code2Message is the map
type Code2Message map[string]string

// UnmarshalJSON parse the json file and populate Config map
func (t *Code2Message) UnmarshalJSON(b []byte) error {

	// Create a local struct that mirrors the data being unmarshalled
	type messageEntry struct {
		Message string `json:"message"`
		Code    string `json:"code"`
	}

	var entries []messageEntry

	// unmarshal the data into the slice
	if err := json.Unmarshal(b, &entries); err != nil {
		return err
	}

	tmp := make(Code2Message)

	// loop over the slice and create the map of entries
	for _, ent := range entries {
		tmp[ent.Code] = ent.Message
	}

	// assign the tmp map to the type
	*t = tmp
	return nil
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}



// Message TODO
type Message map[string]string

// MyData TODO
type MyData map[string][]Message

// MyDataMongoScheme TODO
type MyDataMongoScheme struct {
	Identifier string    `bson:"identifier"`
	Messages   []Message `bson:"messages"`
}

func loadConfig(fileName string) Config {

	configFile, err := ioutil.ReadFile(fileName)
	check(err)
	//fmt.Print(string(configFile))

	var c Config

	if err := json.Unmarshal([]byte(string(configFile)), &c); err != nil {
		fmt.Println(err)
	}

	fmt.Println("Config file loaded.")

	return c
}

func loadMessages(filename string) Code2Message {

	messagesFile, err := ioutil.ReadFile("/tmp/messages.json")
	check(err)

	var code2message Code2Message
	if err := json.Unmarshal([]byte(string(messagesFile)), &code2message); err != nil {
		fmt.Println(err)
	}

	fmt.Println("Code to message config loaded.")

	return code2message
}

func parseFile(fileName string, c Config, code2message Code2Message) MyData {

	file, err := os.Open(fileName)
	check(err)
	defer file.Close()

	mydata := make(MyData)

	scanner := bufio.NewScanner(file)	

	for scanner.Scan() {

		// one line from the raw text file
		line := scanner.Text()
		
		message := parseLine(line, c, code2message)

		if message["isin"] != "" {
			mydata[message["isin"]] = append(mydata[message["isin"]], message)
		}
		//fmt.Println(message)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return mydata
}

func parseLine(line string, c Config, code2message Code2Message) Message {

		var code string = line[15:19]
		//fmt.Println(code)

		var messageID string = code2message[code]
		//fmt.Println(message)

		var level string = string(line[19])
		//fmt.Println(level)

		// the schema of the message
		schema := c[messageID+level]

		// parsed line
		message := make(Message)

		for _, entry := range schema {

			var value string = line[entry.Start-1 : entry.End]

			//fmt.Printf("%s, %T", tmp, tmp)
			message[entry.Name] = value

		}

		return message
}

func main() {

	c := loadConfig("/tmp/config.json") 

	code2message := loadMessages("/tmp/messages.json")
	


    
	start := time.Now()

	mydata := parseFile("/tmp/data.txt", c, code2message)

	elapsed := time.Since(start)
	log.Printf("File parsing took %s", elapsed)

	fmt.Println("Connecting to Mongodb ...")

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))

	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer cancel()

	collection := client.Database("testing").Collection("foo")

	start = time.Now()

	// create the slice of write models
	var writes []mongo.WriteModel

	for k, v := range mydata {
		//fmt.Println("key: ", k, "value: ", v)

		var mydoc MyDataMongoScheme
		mydoc.Identifier = k
		mydoc.Messages = v

		model := mongo.NewInsertOneModel().SetDocument(mydoc)
		writes = append(writes, model)
	}

	// run bulk write
	res, err := collection.BulkWrite(ctx, writes)
	if err != nil {
		log.Fatal(err)
	}

	elapsed = time.Since(start)
	log.Printf("Bulk write took %s", elapsed)

	fmt.Printf(
		"insert: %d, updated: %d, deleted: %d",
		res.InsertedCount,
		res.ModifiedCount,
		res.DeletedCount,
	)

}
