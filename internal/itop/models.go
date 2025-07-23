package itop

import "time"

type Ticket struct {
	ID                 string
	Ref                string
	Title              string
	Status             string
	Class              string // e.g. "Incident"
	Service            string // service_name
	ServiceSubcategory string // service_subcategory_name
	SLA                string // not used for now
	SLTResponse        time.Duration
	SLTResolve         time.Duration
	TimeToResponse     time.Duration
	TimeToResolve      time.Duration
	StartDate          time.Time
	AssignmentDate     time.Time
	ResolutionDate     time.Time
	TTODeadline        time.Time
	TTRDeadline        time.Time
	SLATTOPassed       string
	SLATTRPassed       string
	Agent              string
	Team               string
	Priority           string
	Urgency            string
	Impact             string
	ServiceID          string
	AgentID            string
	TeamID             string
	TicketType         string // for future multi-class
	Caller             string // caller_id_friendlyname
	Origin             string // origin
}
