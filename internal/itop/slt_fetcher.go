package itop

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	sltCache   = make(map[string]SLTDeadline)
	sltCacheMu sync.RWMutex
)

// GetSLTDeadlineCached returns SLTDeadline from cache or fetches from iTop if not cached
func GetSLTDeadlineCached(class, priority, serviceName string) (SLTDeadline, error) {
	key := class + "|" + priority + "|" + serviceName
	sltCacheMu.RLock()
	if val, ok := sltCache[key]; ok {
		sltCacheMu.RUnlock()
		return val, nil
	}
	sltCacheMu.RUnlock()
	slt, err := GetTicketSLT(class, "", priority, serviceName)
	if err == nil {
		sltCacheMu.Lock()
		sltCache[key] = slt
		sltCacheMu.Unlock()
	}
	return slt, err
}

type SLTDeadline struct {
	TTO time.Duration
	TTR time.Duration
}

// GetTicketSLT fetches TTO/TTR for a ticket from iTop (by priority, service_name, class)
func GetTicketSLT(class, ref, priority, serviceName string) (SLTDeadline, error) {
	baseURL := os.Getenv("ITOP_API_URL")
	username := os.Getenv("ITOP_API_USER")
	password := os.Getenv("ITOP_API_PWD")
	if baseURL == "" || username == "" || password == "" {
		return SLTDeadline{}, nil
	}
	// 1. Get SLA_NAME for service_name
	payload1 := map[string]interface{}{
		"operation":     "core/get",
		"class":         "CustomerContract",
		"key":           "SELECT CustomerContract",
		"output_fields": "services_list",
	}
	jsonData1, _ := json.Marshal(payload1)
	form1 := map[string]string{
		"version":   "1.3",
		"auth_user": username,
		"auth_pwd":  password,
		"json_data": string(jsonData1),
	}
	formData1 := encodeForm(form1)
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	req1, _ := http.NewRequest("POST", baseURL, bytes.NewReader(formData1))
	req1.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	// log debug dimatikan
	resp1, err := client.Do(req1)
	if err != nil {
		return SLTDeadline{}, err
	}
	defer resp1.Body.Close()
	body1, _ := ioutil.ReadAll(resp1.Body)
	// log debug dimatikan
	var cc struct {
		Objects map[string]struct {
			Fields struct {
				ServicesList []struct {
					ServiceName string `json:"service_name"`
					SLAName     string `json:"sla_name"`
				} `json:"services_list"`
			} `json:"fields"`
		} `json:"objects"`
	}
	_ = json.Unmarshal(body1, &cc)
	var slaName string
	for _, obj := range cc.Objects {
		for _, svc := range obj.Fields.ServicesList {
			if strings.EqualFold(svc.ServiceName, serviceName) {
				slaName = svc.SLAName
				break
			}
		}
	}
	if slaName == "" {
		return SLTDeadline{}, nil
	}
	// 2. Get SLT for priority, class, sla_name
	requestType := ""
	if class == "Incident" {
		requestType = "incident"
	} else if class == "UserRequest" {
		requestType = "service_request"
	}
	payload2 := map[string]interface{}{
		"operation":     "core/get",
		"class":         "SLT",
		"key":           "SELECT SLT WHERE priority = " + priority + " AND request_type = \"" + requestType + "\"",
		"output_fields": "*",
	}
	jsonData2, _ := json.Marshal(payload2)
	form2 := map[string]string{
		"version":   "1.3",
		"auth_user": username,
		"auth_pwd":  password,
		"json_data": string(jsonData2),
	}
	formData2 := encodeForm(form2)
	req2, _ := http.NewRequest("POST", baseURL, bytes.NewReader(formData2))
	req2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	// log debug dimatikan
	resp2, err := client.Do(req2)
	if err != nil {
		return SLTDeadline{}, err
	}
	defer resp2.Body.Close()
	body2, _ := ioutil.ReadAll(resp2.Body)
	// log debug dimatikan
	var sltResp struct {
		Objects map[string]struct {
			Fields struct {
				Metric   string `json:"metric"`
				Value    string `json:"value"`
				Unit     string `json:"unit"`
				SLAsList []struct {
					SLAName string `json:"sla_name"`
				} `json:"slas_list"`
			} `json:"fields"`
		} `json:"objects"`
	}
	_ = json.Unmarshal(body2, &sltResp)
	var tto, ttr time.Duration
	for _, obj := range sltResp.Objects {
		for _, sla := range obj.Fields.SLAsList {
			if sla.SLAName == slaName {
				valInt := 0
				if obj.Fields.Value != "" {
					if v, err := strconv.Atoi(obj.Fields.Value); err == nil {
						valInt = v
					}
				}
				if obj.Fields.Metric == "tto" {
					tto = parseSLTDuration(valInt, obj.Fields.Unit)
				} else if obj.Fields.Metric == "ttr" {
					ttr = parseSLTDuration(valInt, obj.Fields.Unit)
				}
			}
		}
	}
	return SLTDeadline{TTO: tto, TTR: ttr}, nil
}

func encodeForm(form map[string]string) []byte {
	var buf bytes.Buffer
	for k, v := range form {
		buf.WriteString(k + "=" + v + "&")
	}
	b := buf.Bytes()
	if len(b) > 0 {
		b = b[:len(b)-1]
	}
	return b
}

func parseSLTDuration(val int, unit string) time.Duration {
	switch unit {
	case "hours", "hour", "h":
		return time.Duration(val) * time.Hour
	case "minutes", "minute", "m":
		return time.Duration(val) * time.Minute
	case "seconds", "second", "s":
		return time.Duration(val) * time.Second
	case "days", "day", "d":
		return time.Duration(val) * 24 * time.Hour
	}
	return 0
}
