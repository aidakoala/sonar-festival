package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

const csvFile string = "no-dups-day1.csv"

// const csvFileDay1 string = "no-dups-day1.csv"
const conactsDay1Csv string = "contacts-day1.csv"
const conactsDay2Csv string = "contacts-day2.csv"
const conactsDay3Csv string = "contacts-day3.csv"
const idsDay1Csv string = "mac-to-id-data-day1.csv"
const idsDay2Csv string = "mac-to-id-data-day2.csv"
const idsDay3Csv string = "mac-to-id-data-day3.csv"
const macAddr int = 0
const loc int = 1
const timestamp int = 2
const day1 int = 18
const day2 int = 19
const day3 int = 20

// subtract 3 hours from the timestamp because while
// converting the timestamp to unix time, it does so
// in regards to the local time which is to utc + 3
const utc3Hours = 3 * 60 * 60

var result [][]string

var writer1 *csv.Writer
var writer2 *csv.Writer
var writer3 *csv.Writer

type EventRecord struct {
	nodeId int
	start  int64
	end    int64
}

var day1Map map[string][]EventRecord
var day2Map map[string][]EventRecord
var day3Map map[string][]EventRecord

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func createContacts(myMap map[string][]EventRecord, writer *csv.Writer, wg *sync.WaitGroup) {
	defer wg.Done()

	var contacts, nodeContacts int64
	// var nodeContactLimit int64 = 1000

	err := writer.Write([]string{
		"id1", "id2", "tstart", "tend", "location",
	})
	if err != nil {
		panic(err)
	}
	for key, mySlice := range myMap {
		contacts = 0
		for i := 0; i < len(mySlice)-1; i++ {
			nodeContacts = 0
			nodeId := mySlice[i].nodeId
			for j := i + 1; j < len(mySlice); j++ {
				// if nodeContacts >= nodeContactLimit {
				// 	break
				// }
				// this condition is necessary because a node might return
				// multiple times during a day at a certain location
				if nodeId == mySlice[j].nodeId {
					continue
				}
				// if node A arrives at the location X
				// and node B was already there, create a
				// contact opportunity
				// take into account that my start time
				// has to be before the node B leaves
				if (mySlice[i].start > mySlice[j].start) &&
					(mySlice[i].start < mySlice[j].end) {
					nodeContacts++
					contacts++
					err = writer.Write([]string{
						strconv.Itoa(nodeId),
						strconv.Itoa(mySlice[j].nodeId),
						strconv.FormatInt(mySlice[i].start, 10),
						strconv.FormatInt(min(mySlice[i].end, mySlice[j].end), 10),
						key})
					if err != nil {
						panic(err)
					}
				}
			}
		}
		fmt.Println("no_nodes", strconv.Itoa(len(mySlice)), "location", key,
			"contacts", strconv.FormatInt(contacts, 10))
	}

	writer.Flush()
}

func initIdWriters() []*os.File {
	var files []*os.File

	idsFile1, err := os.OpenFile(idsDay1Csv, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		panic(err)
	}
	files = append(files, idsFile1)
	writer1 = csv.NewWriter(idsFile1)
	err = writer1.Write([]string{
		"id", "location", "timestamp",
	})
	idsFile2, err := os.OpenFile(idsDay2Csv, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		panic(err)
	}
	files = append(files, idsFile2)
	writer2 = csv.NewWriter(idsFile2)
	err = writer1.Write([]string{
		"id", "location", "timestamp",
	})
	idsFile3, err := os.OpenFile(idsDay3Csv, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		panic(err)
	}
	files = append(files, idsFile3)
	writer3 = csv.NewWriter(idsFile3)
	err = writer3.Write([]string{
		"id", "location", "timestamp",
	})

	return files
}

func initContactWriters() []*os.File {
	var files []*os.File

	file, err := os.OpenFile(conactsDay1Csv, os.O_CREATE|os.O_WRONLY, 0660)
	if err != nil {
		panic(err)
	}
	files = append(files, file)
	writer1 = csv.NewWriter(file)
	file, err = os.OpenFile(conactsDay2Csv, os.O_CREATE|os.O_WRONLY, 0660)
	if err != nil {
		panic(err)
	}
	files = append(files, file)
	writer2 = csv.NewWriter(file)
	file, err = os.OpenFile(conactsDay3Csv, os.O_CREATE|os.O_WRONLY, 0660)
	if err != nil {
		panic(err)
	}
	files = append(files, file)
	writer3 = csv.NewWriter(file)

	return files
}

func writeIdCsv(csvLine []string, day int, nodeId string) {
	switch day {
	case day1:
		err := writer1.Write([]string{
			nodeId,
			csvLine[loc],
			csvLine[timestamp],
		})
		if err != nil {
			panic(err)
		}
		break
	case day2:
		err := writer2.Write([]string{
			nodeId,
			csvLine[loc],
			csvLine[timestamp],
		})
		if err != nil {
			panic(err)
		}
		break
	case day3:
		err := writer3.Write([]string{
			nodeId,
			csvLine[loc],
			csvLine[timestamp],
		})
		if err != nil {
			panic(err)
		}
		break
	default:
		fmt.Println("bad day format")
	}
}

func main() {
	numbPtr := flag.Int("nodes", 1000, "the number of nodes for mobemu simulation")
	utcPrt := flag.Int64("utcDiff", utc3Hours, "the number to subtract in order to obtain the utc time")
	flag.Parse()
	fmt.Println("nodes =", *numbPtr)

	day1Map = make(map[string][]EventRecord)
	day2Map = make(map[string][]EventRecord)
	day3Map = make(map[string][]EventRecord)

	idFiles := initIdWriters()

	file, err := os.OpenFile(csvFile, os.O_RDWR, 0660)
	if err != nil {
		panic(err)
	}
	reader := csv.NewReader(file)
	// read all records
	result, _ = reader.ReadAll()

	nodeID := 0
	strNodeID := strconv.Itoa(nodeID)
	currentMac := result[1][macAddr]
	result[1][macAddr] = strNodeID
	// convert the timestamp
	layout := "2006-01-02 15:04:05"
	t, err := time.Parse(layout, result[1][timestamp])
	day := t.Day()
	fmt.Println(day)
	location := result[1][loc]
	// convert the timestamp to unix time
	startTime, prevTime := t.Unix()-*utcPrt, t.Unix()-*utcPrt
	fmt.Println(startTime)

	writeIdCsv(result[1], t.Day(), strNodeID)

	for i := 2; i < len(result); i++ {
		if nodeID >= *numbPtr {
			fmt.Println("nodeId = ", nodeID, "break")
			break
		}

		t, err = time.Parse(layout, result[i][timestamp])
		// set the nodeID
		if result[i][macAddr] == currentMac {
			result[i][macAddr] = strNodeID

			// write id to file in order to count nodes for simulation
			writeIdCsv(result[i], t.Day(), strNodeID)
			/*
			 * if the day and the location have not changed,
			 * contiune to look for the moment the node left that
			 * location
			 */
			// fmt.Printf("node %d loc1 %s loc2 %s day1 %d day2 %d\n", nodeID, location, result[i][loc], day, t.Day())
			if result[i][loc] == location && t.Day() == day {
				prevTime = t.Unix() - *utcPrt
				continue
			} else {
				/*
				 * the node was spotted only once at the scene
				 * and that information is useless, thus we
				 * prepare the  variables for the next day or
				 * location
				 */
				if prevTime == startTime {
					location = result[i][loc]
					day = t.Day()
					startTime, prevTime = t.Unix()-*utcPrt, t.Unix()-*utcPrt
					continue
				} else {
					event := EventRecord{
						nodeID,
						startTime,
						prevTime,
					}

					switch day {
					case day1:
						// fmt.Printf("ADD node %d loc %s day %d startT %d endT %d\n", nodeID, location, day, startTime, prevTime)
						day1Map[location] = append(day1Map[location], event)
						break
					case day2:
						// fmt.Printf("ADD node %d loc %s day %d startT %d endT %d\n", nodeID, location, day, startTime, prevTime)
						day2Map[location] = append(day2Map[location], event)
						break
					case day3:
						// fmt.Printf("ADD node %d loc %s day %d startT %d endT %d\n", nodeID, location, day, startTime, prevTime)
						day3Map[location] = append(day3Map[location], event)
						break
					default:
						fmt.Println("bad day format")
					}

					location = result[i][loc]
					day = t.Day()
					startTime, prevTime = t.Unix()-*utcPrt, t.Unix()-*utcPrt
				}
			}
		} else {
			currentMac = result[i][macAddr]
			nodeID++
			strNodeID = strconv.Itoa(nodeID)
			result[i][macAddr] = strNodeID

			// write id to file in order to count nodes for simulation
			writeIdCsv(result[i], t.Day(), strNodeID)

			if prevTime == startTime {
				location = result[i][loc]
				day = t.Day()
				startTime, prevTime = t.Unix()-*utcPrt, t.Unix()-*utcPrt
				continue
			} else {
				lastID := nodeID - 1
				event := EventRecord{
					lastID,
					startTime,
					prevTime,
				}
				switch day {
				case day1:
					// fmt.Printf("ADD node %d loc %s day %d startT %d endT %d\n", lastID, location, day, startTime, prevTime)
					day1Map[location] = append(day1Map[location], event)
					break
				case day2:
					// fmt.Printf("ADD node %d loc %s day %d startT %d endT %d\n", lastID, location, day, startTime, prevTime)
					day2Map[location] = append(day2Map[location], event)
					break
				case day3:
					// fmt.Printf("ADD node %d loc %s day %d startT %d endT %d\n", lastID, location, day, startTime, prevTime)
					day3Map[location] = append(day3Map[location], event)
					break
				default:
					fmt.Println("bad day format")
				}

				// set the variables for the new node
				location = result[i][loc]
				day = t.Day()
				startTime, prevTime = t.Unix()-*utcPrt, t.Unix()-*utcPrt
			}
		}
	}

	fmt.Println("No of nodes = ", nodeID)

	writer1.Flush()
	writer2.Flush()
	writer3.Flush()
	idFiles[0].Close()
	idFiles[1].Close()
	idFiles[2].Close()
	file.Close()

	contactFiles := initContactWriters()

	// use a WaitGroup to sync all 3 goroutines
	var wg sync.WaitGroup
	// establish contacts between nodes
	wg.Add(1)
	go createContacts(day1Map, writer1, &wg)
	// wg.Add(1)
	// go createContacts(day2Map, writer2, &wg)
	// wg.Add(1)
	// go createContacts(day3Map, writer3, &wg)

	contactFiles[0].Close()
	contactFiles[1].Close()
	contactFiles[2].Close()
}
