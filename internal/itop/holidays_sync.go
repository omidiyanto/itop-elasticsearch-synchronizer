package itop

import (
	"io/ioutil"
	"log"
	"strings"
	"time"
)

type holidayResp struct {
	Objects map[string]struct {
		Fields struct {
			Date string `json:"date"`
		} `json:"fields"`
	} `json:"objects"`
}

// SyncHolidaysToFile periodically fetches holidays from iTop and writes to file (using env vars)
func SyncHolidaysToFile(filePath string, interval time.Duration) {
	go func() {
		for {
			list, err := FetchHolidays()
			if err != nil {
				log.Printf("Failed to fetch holidays: %v", err)
			} else {
				if err := ioutil.WriteFile(filePath, []byte(joinLines(list)), 0644); err != nil {
					log.Printf("Failed to write holidays.txt: %v", err)
				}
			}
			time.Sleep(interval)
		}
	}()
}

// fetchHolidays removed: use FetchHolidays from holiday_fetcher.go

func joinLines(list []string) string {
	return strings.Join(list, "\n") + "\n"
}
