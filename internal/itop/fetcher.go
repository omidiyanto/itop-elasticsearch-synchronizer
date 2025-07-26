package itop

import (
	"encoding/json"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// FetchTicketsByClass fetches tickets for a single class only
func FetchTicketsByClass(class string) ([]Ticket, error) {
	baseURL := os.Getenv("ITOP_API_URL")
	username := os.Getenv("ITOP_API_USER")
	password := os.Getenv("ITOP_API_PWD")
	if baseURL == "" || username == "" || password == "" {
		log.Println("Missing iTop API environment variables")
		return nil, nil
	}
	client := ITopClient{
		BaseURL:  baseURL,
		Username: username,
		Password: password,
		Version:  "1.3",
	}
	params := map[string]interface{}{
		"class":         class,
		"key":           "SELECT " + class,
		"output_fields": "id,ref,title,origin,status,priority,urgency,impact,service_id,service_name,servicesubcategory_name,agent_id,agent_id_friendlyname,team_id,team_id_friendlyname,caller_id_friendlyname,start_date,assignment_date,resolution_date,sla_tto_passed,sla_ttr_passed",
	}
	resp, err := client.Post("core/get", params)
	if err != nil {
		log.Printf("Error from iTop API (%s): %v", class, err)
		return nil, err
	}
	tickets, err := ParseTickets(resp)
	for i := range tickets {
		tickets[i].Class = class
	}
	return tickets, err
}

// FetchTickets fetches tickets from iTop REST API
func FetchTickets() ([]Ticket, error) {
	baseURL := os.Getenv("ITOP_API_URL")
	username := os.Getenv("ITOP_API_USER")
	password := os.Getenv("ITOP_API_PWD")
	if baseURL == "" || username == "" || password == "" {
		log.Println("Missing iTop API environment variables")
		return nil, nil
	}
	client := ITopClient{
		BaseURL:  baseURL,
		Username: username,
		Password: password,
		Version:  "1.3",
	}
	classes := []string{"Incident", "UserRequest"}
	var allTickets []Ticket
	for _, class := range classes {
		params := map[string]interface{}{
			"class":         class,
			"key":           "SELECT " + class,
			"output_fields": "id,ref,title,origin,status,priority,urgency,impact,service_id,service_name,servicesubcategory_name,agent_id,agent_id_friendlyname,team_id,team_id_friendlyname,caller_id_friendlyname,start_date,assignment_date,resolution_date,sla_tto_passed,sla_ttr_passed",
		}
		resp, err := client.Post("core/get", params)
		if err != nil {
			// log.Printf("Error from iTop API (%s): %v", class, err)
			continue
		}
		// log.Printf("Raw iTop API response (%s): %s", class, string(resp))
		tickets, err := ParseTickets(resp)
		log.Printf("Parsed %d tickets from iTop (%s)", len(tickets), class)
		// Set class for each ticket
		for i := range tickets {
			tickets[i].Class = class
		}
		allTickets = append(allTickets, tickets...)
	}
	return allTickets, nil
}

// personTeamCache caches person team information to avoid redundant API calls
var personTeamCache = make(map[string]string)
var personTeamCacheMutex sync.RWMutex

// rateLimiter helps control the frequency of API calls
var rateLimiter *time.Ticker

// init initializes the rate limiter
func init() {
	// Default rate limit: 200ms between requests (5 requests per second)
	rateLimit := 200 * time.Millisecond

	// Allow configuration via environment variable
	if val := os.Getenv("ITOP_API_RATE_LIMIT_MS"); val != "" {
		if ms, err := time.ParseDuration(val + "ms"); err == nil && ms > 0 {
			rateLimit = ms
			log.Printf("Using custom API rate limit: %v", rateLimit)
		}
	}

	rateLimiter = time.NewTicker(rateLimit)
}

// FetchPersonTeams fetches team information for a person by their friendly name
func FetchPersonTeams(personName string) (string, error) {
	// Check cache first
	personTeamCacheMutex.RLock()
	if team, found := personTeamCache[personName]; found {
		personTeamCacheMutex.RUnlock()
		return team, nil
	}
	personTeamCacheMutex.RUnlock()

	// Handle empty name
	if personName == "" {
		personTeamCacheMutex.Lock()
		personTeamCache[personName] = "-"
		personTeamCacheMutex.Unlock()
		return "-", nil
	}

	// Rate limit API calls
	<-rateLimiter.C

	// Escape special characters in the person name for the query
	escapedName := strings.ReplaceAll(personName, "\"", "\\\"")

	baseURL := os.Getenv("ITOP_API_URL")
	username := os.Getenv("ITOP_API_USER")
	password := os.Getenv("ITOP_API_PWD")
	if baseURL == "" || username == "" || password == "" {
		log.Println("Missing iTop API environment variables")
		return "-", nil
	}
	client := ITopClient{
		BaseURL:  baseURL,
		Username: username,
		Password: password,
		Version:  "1.3",
	}
	params := map[string]interface{}{
		"class":         "Person",
		"key":           "SELECT Person WHERE friendlyname=\"" + escapedName + "\"",
		"output_fields": "friendlyname,team_list",
	}
	resp, err := client.Post("core/get", params)
	if err != nil {
		log.Printf("Error fetching person teams: %v", err)
		return "-", err
	}

	var result struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Objects map[string]struct {
			Fields struct {
				TeamList []struct {
					TeamName string `json:"team_name"`
				} `json:"team_list"`
			} `json:"fields"`
		} `json:"objects"`
	}

	if err := json.Unmarshal(resp, &result); err != nil {
		log.Printf("Error parsing person teams response: %v", err)
		return "-", err
	}

	if result.Code != 0 || len(result.Objects) == 0 {
		personTeamCacheMutex.Lock()
		personTeamCache[personName] = "-" // Cache negative result
		personTeamCacheMutex.Unlock()
		return "-", nil
	}

	var teamNames []string
	for _, obj := range result.Objects {
		for _, team := range obj.Fields.TeamList {
			teamNames = append(teamNames, team.TeamName)
		}
	}

	if len(teamNames) == 0 {
		personTeamCacheMutex.Lock()
		personTeamCache[personName] = "-" // Cache empty result
		personTeamCacheMutex.Unlock()
		return "-", nil
	}

	teamList := strings.Join(teamNames, ", ")
	personTeamCacheMutex.Lock()
	personTeamCache[personName] = teamList // Cache the result
	personTeamCacheMutex.Unlock()
	return teamList, nil
}
