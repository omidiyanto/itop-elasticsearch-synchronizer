package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	itop "itop-sla-exporter/internal/itop"
	utils "itop-sla-exporter/internal/utils"

	"github.com/joho/godotenv"
)

// ESConfig holds elasticsearch connection info
type ESConfig struct {
	URL      string
	Username string
	Password string
	Index    string
}

// ESTicket is the model for elasticsearch
type ESTicket struct {
	ID                                string     `json:"id"`
	Ref                               string     `json:"ref"`
	Class                             string     `json:"class"`
	Title                             string     `json:"title"`
	Status                            string     `json:"status"`
	Priority                          string     `json:"priority"`
	Urgency                           string     `json:"urgency"`
	Impact                            string     `json:"impact"`
	ServiceID                         string     `json:"service_id"`
	ServiceName                       string     `json:"service_name"`
	ServiceSubcategoryName            string     `json:"servicesubcategory_name"`
	AgentID                           string     `json:"agent_id"`
	Agent                             string     `json:"agent_id_friendlyname"`
	TeamID                            string     `json:"team_id"`
	Team                              string     `json:"team_id_friendlyname"`
	Caller                            string     `json:"caller_id_friendlyname"`
	CallerTeam                        string     `json:"caller_team"` // Team(s) of the caller, comma-separated if multiple teams
	Origin                            string     `json:"origin"`
	StartDate                         *time.Time `json:"start_date,omitempty"`
	AssignmentDate                    *time.Time `json:"assignment_date,omitempty"`
	ResolutionDate                    *time.Time `json:"resolution_date,omitempty"`
	TimeToResponseRaw                 float64    `json:"time_to_response_raw"`
	TimeToResolveRaw                  float64    `json:"time_to_resolve_raw"`
	SLAComplianceResponseRaw          string     `json:"sla_compliance_response_raw"`
	SLAComplianceResolveRaw           string     `json:"sla_compliance_resolve_raw"`
	TimeToResponseBusinessHr          float64    `json:"time_to_response_business_hour"`
	TimeToResolveBusinessHr           float64    `json:"time_to_resolve_business_hour"`
	SLAComplianceResponseBusinessHour string     `json:"sla_compliance_response_bussiness_hour"`
	SLAComplianceResolveBusinessHour  string     `json:"sla_compliance_resolve_bussiness_hour"`

	TimeToResponse24BH        float64 `json:"time_to_response_24bh"`
	TimeToResolve24BH         float64 `json:"time_to_resolve_24bh"`
	SLAComplianceResponse24BH string  `json:"sla_compliance_response_24bh"`
	SLAComplianceResolve24BH  string  `json:"sla_compliance_resolve_24bh"`
}

func main() {
	// Load .env if exists, ignore error if not found
	_ = godotenv.Load()

	esConf := ESConfig{
		URL:      os.Getenv("ELASTIC_URL"),
		Username: os.Getenv("ELASTIC_USER"),
		Password: os.Getenv("ELASTIC_PWD"),
		Index:    os.Getenv("ELASTIC_INDEX"),
	}

	if esConf.URL == "" || esConf.Index == "" {
		log.Fatal("Missing ELASTIC_URL or ELASTIC_INDEX env var")
	}

	// Debug mode
	debug := os.Getenv("DEBUG") == "true"

	// Sync holidays from iTop to file in background (periodic, setiap 10 detik)
	go itop.SyncHolidaysToFile("holidays.txt", 10*time.Second)

	go syncLoop(esConf, debug)
	select {} // block forever
}

func syncLoop(esConf ESConfig, debug bool) {
	interval := 3 * time.Second
	if s := os.Getenv("SYNC_INTERVAL"); s != "" {
		if d, err := time.ParseDuration(s); err == nil {
			interval = d
		}
	}
	for {
		// Load holidays
		holidays, _ := itop.LoadHolidaysFromFile("holidays.txt")
		holidayMap := make(map[string]struct{})
		for _, h := range holidays {
			holidayMap[h] = struct{}{}
		}

		// Fetch Incident & UserRequest tickets concurrently
		type result struct {
			class   string
			tickets []itop.Ticket
			err     error
		}
		classes := []string{"Incident", "UserRequest"}
		ch := make(chan result, len(classes))
		for _, class := range classes {
			go func(class string) {
				tickets, err := itop.FetchTicketsByClass(class)
				ch <- result{class, tickets, err}
			}(class)
		}
		var allTickets []itop.Ticket
		countByClass := map[string]int{}
		for i := 0; i < len(classes); i++ {
			r := <-ch
			if r.err != nil {
				log.Printf("Failed to fetch tickets from iTop (%s): %v", r.class, r.err)
				continue
			}
			countByClass[r.class] = len(r.tickets)
			allTickets = append(allTickets, r.tickets...)
		}
		log.Printf("Parsed %d tickets (Incident) and %d tickets (UserRequest)", countByClass["Incident"], countByClass["UserRequest"])

		// Fetch all tickets from Elasticsearch (by scroll or search all)
		esTickets := fetchAllESTickets(esConf)
		esTicketMap := make(map[string]ESTicket)
		for _, t := range esTickets {
			esTicketMap[hashTicketKey(t.ID, t.Ref, t.Class)] = t
		}

		// Sync tickets
		for _, t := range allTickets {
			key := hashTicketKey(t.ID, t.Ref, t.Class)
			est := mapTicketToES(t, holidayMap, debug)
			// Compare, if not exist or different, upsert
			if old, ok := esTicketMap[key]; !ok || !compareESTicket(est, old) {
				upsertESTicket(esConf, est)
			}
			// Remove from map to track which to delete
			delete(esTicketMap, key)
		}
		// Delete tickets in ES that no longer exist in iTop
		for _, t := range esTicketMap {
			deleteESTicket(esConf, t)
		}
		// log.Println("Sync complete at", time.Now().Format(time.RFC3339))
		time.Sleep(interval)
	}
}

func hashTicketKey(id, ref, class string) string {
	h := sha1.New()
	h.Write([]byte(id + ":" + ref + ":" + class))
	return hex.EncodeToString(h.Sum(nil))
}

func mapTicketToES(t itop.Ticket, holidays map[string]struct{}, debug bool) ESTicket {
	workStart := os.Getenv("WORK_START")
	workEnd := os.Getenv("WORK_END")
	if workStart == "" {
		workStart = "08:00"
	}
	if workEnd == "" {
		workEnd = "17:00"
	}
	ttrRaw := t.TimeToResolve.Seconds()
	ttoRaw := t.TimeToResponse.Seconds()
	ttrBH := utils.CalculateBusinessHourDuration(t.StartDate, t.ResolutionDate, workStart, workEnd, holidays)
	ttoBH := utils.CalculateBusinessHourDuration(t.StartDate, t.AssignmentDate, workStart, workEnd, holidays)

	// 24-hour business hour calculation (00:00-23:59)
	ttr24BH := utils.CalculateBusinessHourDuration(t.StartDate, t.ResolutionDate, "00:00", "23:59", holidays)
	tto24BH := utils.CalculateBusinessHourDuration(t.StartDate, t.AssignmentDate, "00:00", "23:59", holidays)

	// Fetch caller team information
	callerTeam := "-"
	if t.Caller != "" {
		var err error
		callerTeam, err = itop.FetchPersonTeams(t.Caller)
		if err != nil {
			log.Printf("Error fetching teams for caller %s: %v", t.Caller, err)
		} else if callerTeam != "-" && debug {
			log.Printf("Found teams for caller %s: %s", t.Caller, callerTeam)
		}
	}

	// Ambil SLT dari iTop (cache)
	slt, _ := itop.GetSLTDeadlineCached(t.Class, t.Priority, t.Service)

	// Compliance logic (RAW)
	var slaComplianceResponseRaw, slaComplianceResolveRaw string
	if slt.TTO > 0 && ttoRaw > 0 {
		if ttoRaw <= slt.TTO.Seconds() {
			slaComplianceResponseRaw = "comply"
		} else {
			slaComplianceResponseRaw = "overdue"
		}
	} else {
		slaComplianceResponseRaw = ""
	}
	if slt.TTR > 0 && ttrRaw > 0 {
		if ttrRaw <= slt.TTR.Seconds() {
			slaComplianceResolveRaw = "comply"
		} else {
			slaComplianceResolveRaw = "overdue"
		}
	} else {
		slaComplianceResolveRaw = ""
	}

	// Compliance logic (Business Hour)
	var slaComplianceResponseBH, slaComplianceResolveBH string
	if slt.TTO > 0 {
		if (ttoBH > 0 && ttoBH.Seconds() <= slt.TTO.Seconds()) || (ttoBH.Seconds() == 0 && ttoRaw > 0 && ttoRaw <= slt.TTO.Seconds()) {
			slaComplianceResponseBH = "comply"
		} else if ttoBH.Seconds() > 0 {
			slaComplianceResponseBH = "overdue"
		} else {
			slaComplianceResponseBH = ""
		}
	} else {
		slaComplianceResponseBH = ""
	}
	if slt.TTR > 0 {
		if (ttrBH > 0 && ttrBH.Seconds() <= slt.TTR.Seconds()) || (ttrBH.Seconds() == 0 && ttrRaw > 0 && ttrRaw <= slt.TTR.Seconds()) {
			slaComplianceResolveBH = "comply"
		} else if ttrBH.Seconds() > 0 {
			slaComplianceResolveBH = "overdue"
		} else {
			slaComplianceResolveBH = ""
		}
	} else {
		slaComplianceResolveBH = ""
	}

	// Compliance logic (24-hour business hour)
	var slaComplianceResponse24BH, slaComplianceResolve24BH string
	if slt.TTO > 0 {
		if (tto24BH > 0 && tto24BH.Seconds() <= slt.TTO.Seconds()) || (tto24BH.Seconds() == 0 && ttoRaw > 0 && ttoRaw <= slt.TTO.Seconds()) {
			slaComplianceResponse24BH = "comply"
		} else if tto24BH.Seconds() > 0 {
			slaComplianceResponse24BH = "overdue"
		} else {
			slaComplianceResponse24BH = ""
		}
	} else {
		slaComplianceResponse24BH = ""
	}
	if slt.TTR > 0 {
		if (ttr24BH > 0 && ttr24BH.Seconds() <= slt.TTR.Seconds()) || (ttr24BH.Seconds() == 0 && ttrRaw > 0 && ttrRaw <= slt.TTR.Seconds()) {
			slaComplianceResolve24BH = "comply"
		} else if ttr24BH.Seconds() > 0 {
			slaComplianceResolve24BH = "overdue"
		} else {
			slaComplianceResolve24BH = ""
		}
	} else {
		slaComplianceResolve24BH = ""
	}

	tz := os.Getenv("TIMEZONE")
	if tz == "" {
		tz = "UTC"
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		loc = time.Local
	}

	var startDatePtr, assignmentDatePtr, resolutionDatePtr *time.Time
	if !t.StartDate.IsZero() {
		v := t.StartDate.In(loc)
		startDatePtr = &v
	}
	if !t.AssignmentDate.IsZero() {
		v := t.AssignmentDate.In(loc)
		assignmentDatePtr = &v
	}
	if !t.ResolutionDate.IsZero() {
		v := t.ResolutionDate.In(loc)
		resolutionDatePtr = &v
	}

	return ESTicket{
		ID:                                t.ID,
		Ref:                               t.Ref,
		Class:                             t.Class,
		Title:                             t.Title,
		Status:                            t.Status,
		Priority:                          priorityLabel(t.Priority),
		Urgency:                           urgencyLabel(t.Urgency),
		Impact:                            impactLabel(t.Impact),
		ServiceID:                         t.ServiceID,
		ServiceName:                       t.Service,
		ServiceSubcategoryName:            t.ServiceSubcategory,
		AgentID:                           t.AgentID,
		Agent:                             t.Agent,
		TeamID:                            t.TeamID,
		Team:                              t.Team,
		Caller:                            t.Caller,
		CallerTeam:                        callerTeam,
		Origin:                            t.Origin,
		StartDate:                         startDatePtr,
		AssignmentDate:                    assignmentDatePtr,
		ResolutionDate:                    resolutionDatePtr,
		TimeToResponseRaw:                 ttoRaw,
		TimeToResolveRaw:                  ttrRaw,
		SLAComplianceResponseRaw:          slaComplianceResponseRaw,
		SLAComplianceResolveRaw:           slaComplianceResolveRaw,
		TimeToResponseBusinessHr:          ttoBH.Seconds(),
		TimeToResolveBusinessHr:           ttrBH.Seconds(),
		SLAComplianceResponseBusinessHour: slaComplianceResponseBH,
		SLAComplianceResolveBusinessHour:  slaComplianceResolveBH,
		TimeToResponse24BH:                tto24BH.Seconds(),
		TimeToResolve24BH:                 ttr24BH.Seconds(),
		SLAComplianceResponse24BH:         slaComplianceResponse24BH,
		SLAComplianceResolve24BH:          slaComplianceResolve24BH,
	}
}

func priorityLabel(id string) string {
	switch id {
	case "1":
		return "Critical"
	case "2":
		return "High"
	case "3":
		return "Medium"
	case "4":
		return "Low"
	default:
		return id
	}
}

func urgencyLabel(id string) string {
	switch id {
	case "1":
		return "Critical"
	case "2":
		return "High"
	case "3":
		return "Medium"
	case "4":
		return "Low"
	default:
		return id
	}
}

func impactLabel(id string) string {
	switch id {
	case "1":
		return "A department"
	case "2":
		return "A service"
	case "3":
		return "A person"
	default:
		return id
	}
}

func compareESTicket(a, b ESTicket) bool {
	aj, _ := json.Marshal(a)
	bj, _ := json.Marshal(b)
	return bytes.Equal(aj, bj)
}

func fetchAllESTickets(conf ESConfig) []ESTicket {
	// Simple: fetch all (assume <10k)
	url := conf.URL + "/" + conf.Index + "/_search?size=10000"
	req, _ := http.NewRequest("GET", url, nil)
	if conf.Username != "" {
		req.SetBasicAuth(conf.Username, conf.Password)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("Failed to fetch from ES: %v", err)
		return nil
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	var result struct {
		Hits struct {
			Hits []struct {
				Source ESTicket `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	_ = json.Unmarshal(body, &result)
	var out []ESTicket
	for _, h := range result.Hits.Hits {
		out = append(out, h.Source)
	}
	return out
}

func upsertESTicket(conf ESConfig, t ESTicket) {
	// Use hash as _id
	id := hashTicketKey(t.ID, t.Ref, t.Class)
	url := conf.URL + "/" + conf.Index + "/_doc/" + id
	data, _ := json.Marshal(t)
	req, _ := http.NewRequest("PUT", url, bytes.NewReader(data))
	if conf.Username != "" {
		req.SetBasicAuth(conf.Username, conf.Password)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("Failed to upsert ES: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := ioutil.ReadAll(resp.Body)
		log.Printf("ES upsert error: %s", string(body))
	}
}

func deleteESTicket(conf ESConfig, t ESTicket) {
	id := hashTicketKey(t.ID, t.Ref, t.Class)
	url := conf.URL + "/" + conf.Index + "/_doc/" + id
	req, _ := http.NewRequest("DELETE", url, nil)
	if conf.Username != "" {
		req.SetBasicAuth(conf.Username, conf.Password)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("Failed to delete ES: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode != 404 {
		body, _ := ioutil.ReadAll(resp.Body)
		log.Printf("ES delete error: %s", string(body))
	}
}
