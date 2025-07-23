package utils

import (
	"time"
)

// CalculateBusinessHourDuration calculates duration between two times, only counting work hours and excluding holidays.
func CalculateBusinessHourDuration(start, end time.Time, workStart, workEnd string, holidays map[string]struct{}) time.Duration {
	// Defensive: if end < start, return 0
	if end.Before(start) {
		return 0
	}
	// Parse work hours, handle error
	ws, err1 := time.Parse("15:04", workStart)
	we, err2 := time.Parse("15:04", workEnd)
	if err1 != nil || err2 != nil {
		// log error, fallback to full duration
		// fmt.Printf("[BusinessHour] Failed to parse work hours: %v, %v\n", err1, err2)
		return end.Sub(start)
	}
	var total time.Duration
	cur := start
	for cur.Before(end) {
		// Check holiday
		dateStr := cur.Format("2006-01-02")
		if _, isHoliday := holidays[dateStr]; isHoliday {
			cur = time.Date(cur.Year(), cur.Month(), cur.Day()+1, ws.Hour(), ws.Minute(), 0, 0, cur.Location())
			continue
		}
		// Check weekend (Saturday=6, Sunday=0)
		weekday := cur.Weekday()
		if weekday == time.Saturday || weekday == time.Sunday {
			cur = time.Date(cur.Year(), cur.Month(), cur.Day()+1, ws.Hour(), ws.Minute(), 0, 0, cur.Location())
			continue
		}
		// Work hour window
		workDayStart := time.Date(cur.Year(), cur.Month(), cur.Day(), ws.Hour(), ws.Minute(), 0, 0, cur.Location())
		workDayEnd := time.Date(cur.Year(), cur.Month(), cur.Day(), we.Hour(), we.Minute(), 0, 0, cur.Location())
		if cur.Before(workDayStart) {
			cur = workDayStart
		}
		if cur.After(workDayEnd) {
			cur = time.Date(cur.Year(), cur.Month(), cur.Day()+1, ws.Hour(), ws.Minute(), 0, 0, cur.Location())
			continue
		}
		// Next step: either end of workday or end
		next := workDayEnd
		if end.Before(workDayEnd) {
			next = end
		}
		// Defensive: only add if next > cur
		if next.After(cur) {
			total += next.Sub(cur)
		}
		// Debug log (uncomment for troubleshooting)
		// fmt.Printf("[BusinessHour] cur=%v, next=%v, add=%v, total=%v\n", cur, next, next.Sub(cur), total)
		cur = next
		if cur.Before(end) {
			cur = time.Date(cur.Year(), cur.Month(), cur.Day()+1, ws.Hour(), ws.Minute(), 0, 0, cur.Location())
		}
	}
	// Defensive: never return negative
	if total < 0 {
		// fmt.Printf("[BusinessHour] Negative total detected: %v\n", total)
		return 0
	}
	return total
}
