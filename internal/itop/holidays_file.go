package itop

import (
	"bufio"
	"os"
)

// LoadHolidaysFromFile reads holidays from a file (one date per line)
func LoadHolidaysFromFile(filePath string) ([]string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var holidays []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			holidays = append(holidays, line)
		}
	}
	return holidays, scanner.Err()
}
