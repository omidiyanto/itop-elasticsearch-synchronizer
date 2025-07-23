package itop

import (
	"log"
	"os"
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
