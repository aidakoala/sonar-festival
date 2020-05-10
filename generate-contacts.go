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

const csvFileDay1 string = "no-dups-day1.csv"
const csvFileDay2 string = "no-dups-day2.csv"
const csvFileDay3 string = "no-dups-day3.csv"

const durationsDay1Csv = "durations-day1.csv"
const durationsDay2Csv = "durations-day2.csv"
const durationsDay3Csv = "durations-day3.csv"

const conactsDay1Csv string = "contacts-day1.csv"
const conactsDay2Csv string = "contacts-day2.csv"
const conactsDay3Csv string = "contacts-day3.csv"

const idsDay1Csv string = "mac-to-id-data-day1.csv"
const idsDay2Csv string = "mac-to-id-data-day2.csv"
const idsDay3Csv string = "mac-to-id-data-day3.csv"

const macAddr int = 0
const loc int = 1
const timestamp int = 2

// subtract 3 hours from the timestamp because while
// converting the timestamp to unix time, it does so
// in regards to the local time which is to utc + 3
const utc3Hours = 3 * 60 * 60

type EventRecord struct {
	nodeId int
	start  int64
	end    int64
}

type WifiEvent struct {
	event    EventRecord
	location int
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func createContacts(myMap map[int][]EventRecord, csvContactsFile string, day int, wg *sync.WaitGroup) {
	defer wg.Done()

	var contacts, nodeContacts int64
	var nodeContactLimit int64 = 150

	file, err := os.OpenFile(csvContactsFile, os.O_CREATE|os.O_WRONLY, 0660)
	if err != nil {
		panic(err)
	}
	writer := csv.NewWriter(file)

	err = writer.Write([]string{
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
				if nodeContacts >= nodeContactLimit {
					break
				}
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
						strconv.Itoa(key)})
					if err != nil {
						panic(err)
					}
				}
			}
		}
		fmt.Println(day, "no_nodes", strconv.Itoa(len(mySlice)), "location",
			key, "contacts", strconv.FormatInt(contacts, 10))
	}

	writer.Flush()
	file.Close()
}

func macsToIds(utcPrt *int64, csvInFile string, events *[]WifiEvent, wg *sync.WaitGroup) {
	defer wg.Done()
	var result [][]string

	// open csv input file
	file, err := os.OpenFile(csvInFile, os.O_RDWR, 0660)
	if err != nil {
		panic(err)
	}
	reader := csv.NewReader(file)
	// read all records
	result, _ = reader.ReadAll()
	file.Close()

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

	for i := 2; i < len(result); i++ {
		t, err = time.Parse(layout, result[i][timestamp])
		// set the nodeID
		if result[i][macAddr] == currentMac {
			/*
			 * if the day and the location have not changed,
			 * contiune to look for the moment the node left that
			 * location
			 */
			if result[i][loc] == location {
				prevTime = t.Unix() - *utcPrt
				continue
			} else {
				/*
				 * OUTLIER REMOVAL the node was spotted only once at the scene
				 * and that information is useless, thus we
				 * prepare the  variables for the new location
				 */
				if prevTime == startTime {
					location = result[i][loc]
					startTime, prevTime = t.Unix()-*utcPrt, t.Unix()-*utcPrt
					continue
				} else {
					event := EventRecord{
						nodeID,
						startTime,
						prevTime,
					}

					locNum, err := strconv.Atoi(location)
					if err != nil {
						print(err)
					}
					*events = append(*events, WifiEvent{event, locNum})

					location = result[i][loc]
					startTime, prevTime = t.Unix()-*utcPrt, t.Unix()-*utcPrt
				}
			}
		} else {
			currentMac = result[i][macAddr]
			nodeID++
			strNodeID = strconv.Itoa(nodeID)

			if prevTime == startTime {
				// there is no need to generate a new id since the last one
				// was discarded only if there is not a previous entry for
				// nodeID--
				if len(*events) == 0 {
					nodeID--
				} else if (*events)[len(*events)-1].event.nodeId != (nodeID - 1) {
					nodeID--
				}
				location = result[i][loc]
				startTime, prevTime = t.Unix()-*utcPrt, t.Unix()-*utcPrt
				continue
			} else {
				lastID := nodeID - 1
				event := EventRecord{
					lastID,
					startTime,
					prevTime,
				}

				locNum, err := strconv.Atoi(location)
				if err != nil {
					print(err)
				}
				*events = append(*events, WifiEvent{event, locNum})

				// set the variables for the new node
				location = result[i][loc]
				startTime, prevTime = t.Unix()-*utcPrt, t.Unix()-*utcPrt
			}
		}
	}

	fmt.Println(day, "No of nodes = ", nodeID)
}

func computeTimePerDay(events []WifiEvent, csvFile string, durations map[int]int64, blacklist map[int]bool, wg *sync.WaitGroup) {
	defer wg.Done()

	for i := 0; i < len(events)-1; i++ {
		dt := (events[i].event.end - events[i].event.start) / 60
		// if a node spent more than 10 hours at the same location
		// blacklist it
		if dt >= 600 {
			blacklist[events[i].event.nodeId] = true
		}
		// test if there is an enty for nodeId
		_, ok := durations[events[i].event.nodeId]
		if ok {
			durations[events[i].event.nodeId] += dt
		} else {
			durations[events[i].event.nodeId] = dt
		}
	}

	file, err := os.OpenFile(csvFile, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		panic(err)
	}
	writer := csv.NewWriter(file)
	err = writer.Write([]string{
		"id", "duration",
	})

	for id, time := range durations {
		writer.Write([]string{
			strconv.Itoa(id),
			strconv.FormatInt(time, 10),
		})
	}

	writer.Flush()
	file.Close()

	// create a blacklist of nodes who spent less than 60 min at the festival
	for key, val := range durations {
		if val < 60 {
			blacklist[key] = true
		}
	}
}

func macsToIdsBlacklist(utcPrt *int64, numbPtr *int, csvMacToIdFile string, dayMap map[int][]EventRecord, events []WifiEvent, blacklist map[int]bool, day int, wg *sync.WaitGroup) {
	defer wg.Done()

	filteredEvents := make([]WifiEvent, 0)

	// open file to write node ids
	idsFile, err := os.OpenFile(csvMacToIdFile, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		panic(err)
	}
	writer := csv.NewWriter(idsFile)
	err = writer.Write([]string{
		"id", "location", "tstart", "tend",
	})

	// filter out the blacklisted elements
	for i := 0; i < len(events); i++ {
		if !blacklist[events[i].event.nodeId] {
			filteredEvents = append(filteredEvents, events[i])
		}
	}

	// make ids consecutive integers starting from 0
	newId := 0
	currentId := filteredEvents[0].event.nodeId
	for i := 1; i < len(filteredEvents); i++ {
		if filteredEvents[i].event.nodeId == currentId {
			filteredEvents[i].event.nodeId = newId
			dayMap[filteredEvents[i].location] = append(dayMap[filteredEvents[i].location], filteredEvents[i].event)
		} else {
			newId++
			currentId = filteredEvents[i].event.nodeId
			filteredEvents[i].event.nodeId = newId
			dayMap[filteredEvents[i].location] = append(dayMap[filteredEvents[i].location], filteredEvents[i].event)
		}
	}

	var strNodeId string
	for i := 0; i < len(filteredEvents); i++ {
		strNodeId = strconv.Itoa(filteredEvents[i].event.nodeId)
		strLoc := strconv.Itoa(filteredEvents[i].location)
		strStrat := strconv.FormatInt(filteredEvents[i].event.start, 10)
		strEnd := strconv.FormatInt(filteredEvents[i].event.end, 10)
		writer.Write([]string{strNodeId, strLoc, strStrat, strEnd})
	}

	fmt.Println(day, "No of nodes = ", newId)

	writer.Flush()
	idsFile.Close()
}

func main() {
	numbPtr := flag.Int("nodes", 1000, "the number of nodes for mobemu simulation")
	utcPtr := flag.Int64("utcDiff", utc3Hours, "the number to subtract in order to obtain the utc time")
	flag.Parse()
	fmt.Println("nodes =", *numbPtr)

	// use a WaitGroup to sync all 3 goroutines
	var wg sync.WaitGroup

	events1 := make([]WifiEvent, 0)
	events2 := make([]WifiEvent, 0)
	events3 := make([]WifiEvent, 0)
	wg.Add(1)
	macsToIds(utcPtr, csvFileDay1, &events1, &wg)
	wg.Add(1)
	macsToIds(utcPtr, csvFileDay2, &events2, &wg)
	wg.Add(1)
	macsToIds(utcPtr, csvFileDay3, &events3, &wg)
	wg.Wait()

	durations1 := make(map[int]int64)
	durations2 := make(map[int]int64)
	durations3 := make(map[int]int64)
	// blacklists
	blacklist1 := make(map[int]bool)
	blacklist2 := make(map[int]bool)
	blacklist3 := make(map[int]bool)

	// compute the total time a node spent at the festival per day
	wg.Add(1)
	go computeTimePerDay(events1, durationsDay1Csv, durations1, blacklist1, &wg)
	wg.Add(1)
	go computeTimePerDay(events2, durationsDay2Csv, durations2, blacklist2, &wg)
	wg.Add(1)
	go computeTimePerDay(events3, durationsDay3Csv, durations3, blacklist3, &wg)
	wg.Wait()

	day1Map := make(map[int][]EventRecord)
	day2Map := make(map[int][]EventRecord)
	day3Map := make(map[int][]EventRecord)

	wg.Add(1)
	go macsToIdsBlacklist(utcPtr, numbPtr, idsDay1Csv, day1Map, events1, blacklist1, 18, &wg)
	wg.Add(1)
	go macsToIdsBlacklist(utcPtr, numbPtr, idsDay2Csv, day2Map, events2, blacklist2, 19, &wg)
	wg.Add(1)
	go macsToIdsBlacklist(utcPtr, numbPtr, idsDay3Csv, day3Map, events3, blacklist3, 20, &wg)
	wg.Wait()

	wg.Add(1)
	go createContacts(day1Map, conactsDay1Csv, 18, &wg)
	wg.Add(1)
	go createContacts(day2Map, conactsDay2Csv, 19, &wg)
	wg.Add(1)
	go createContacts(day3Map, conactsDay3Csv, 20, &wg)
	wg.Wait()

}
