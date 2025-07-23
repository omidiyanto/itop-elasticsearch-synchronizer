package itop

import (
	"encoding/json"
	"os"
	"time"
)

// parseDateFlexible mencoba beberapa format waktu umum
func parseDateFlexible(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	layouts := []string{
		"2006-01-02 15:04:05",
		time.RFC3339,
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02",
		"2006-01-02 15:04",
	}
	var t time.Time
	var err error
	tz := os.Getenv("TIMEZONE")
	if tz == "" {
		tz = "Asia/Jakarta"
	}
	loc, locErr := time.LoadLocation(tz)
	if locErr != nil {
		loc = time.Local
	}
	for _, layout := range layouts {
		t, err = time.ParseInLocation(layout, s, loc)
		if err == nil {
			return t, nil
		}
	}
	return time.Time{}, err
}

type TicketResponse struct {
	Objects map[string]struct {
		Fields struct {
			ID                     string `json:"id"`
			Ref                    string `json:"ref"`
			Title                  string `json:"title"`
			Status                 string `json:"status"`
			Priority               string `json:"priority"`
			Urgency                string `json:"urgency"`
			Impact                 string `json:"impact"`
			ServiceID              string `json:"service_id"`
			ServiceName            string `json:"service_name"`
			ServiceSubcategoryName string `json:"servicesubcategory_name"`
			AgentID                string `json:"agent_id"`
			Agent                  string `json:"agent_id_friendlyname"`
			TeamID                 string `json:"team_id"`
			Team                   string `json:"team_id_friendlyname"`
			Caller                 string `json:"caller_id_friendlyname"`
			Origin                 string `json:"origin"`
			StartDate              string `json:"start_date"`
			AssignmentDate         string `json:"assignment_date"`
			ResolutionDate         string `json:"resolution_date"`
			TTODeadline            string `json:"tto_deadline"`
			TTRDeadline            string `json:"ttr_deadline"`
			SLATTOPassed           string `json:"sla_tto_passed"`
			SLATTRPassed           string `json:"sla_ttr_passed"`
		} `json:"fields"`
	} `json:"objects"`
}

func ParseTickets(data []byte) ([]Ticket, error) {
	var resp TicketResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	var tickets []Ticket
	for _, obj := range resp.Objects {
		fields := obj.Fields
		startDate, _ := parseDateFlexible(fields.StartDate)
		assignmentDate, _ := parseDateFlexible(fields.AssignmentDate)
		resolutionDate, _ := parseDateFlexible(fields.ResolutionDate)
		ttoDeadline, _ := parseDateFlexible(fields.TTODeadline)
		ttrDeadline, _ := parseDateFlexible(fields.TTRDeadline)

		ticket := Ticket{
			ID:                 fields.ID,
			Ref:                fields.Ref,
			Title:              fields.Title,
			Status:             fields.Status,
			Class:              "Incident",
			Service:            fields.ServiceName,
			ServiceSubcategory: fields.ServiceSubcategoryName,
			StartDate:          startDate,
			AssignmentDate:     assignmentDate,
			ResolutionDate:     resolutionDate,
			TTODeadline:        ttoDeadline,
			TTRDeadline:        ttrDeadline,
			SLATTOPassed:       fields.SLATTOPassed,
			SLATTRPassed:       fields.SLATTRPassed,
			Agent:              fields.Agent,
			AgentID:            fields.AgentID,
			Team:               fields.Team,
			TeamID:             fields.TeamID,
			Priority:           fields.Priority,
			Urgency:            fields.Urgency,
			Impact:             fields.Impact,
			ServiceID:          fields.ServiceID,
			Caller:             fields.Caller,
			Origin:             fields.Origin,
		}
		// Calculate TTO/TTR
		if !assignmentDate.IsZero() && !startDate.IsZero() {
			ticket.TimeToResponse = assignmentDate.Sub(startDate)
		}
		if !resolutionDate.IsZero() && !startDate.IsZero() {
			ticket.TimeToResolve = resolutionDate.Sub(startDate)
		}
		tickets = append(tickets, ticket)
	}
	return tickets, nil
}
