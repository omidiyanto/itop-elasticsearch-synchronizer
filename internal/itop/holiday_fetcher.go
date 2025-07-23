package itop

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

// FetchHolidays fetches holiday dates from iTop REST API using env vars ITOP_API_URL, ITOP_API_USER, ITOP_API_PWD
func FetchHolidays() ([]string, error) {
	baseURL := os.Getenv("ITOP_API_URL")
	username := os.Getenv("ITOP_API_USER")
	password := os.Getenv("ITOP_API_PWD")
	if baseURL == "" || username == "" || password == "" {
		log.Println("Missing iTop API environment variables for holiday fetch")
		return nil, nil
	}
	payload := map[string]interface{}{
		"operation":     "core/get",
		"class":         "Holiday",
		"key":           "SELECT Holiday",
		"output_fields": "date",
	}
	jsonData, _ := json.Marshal(payload)
	form := map[string]string{
		"version":   "1.3",
		"auth_user": username,
		"auth_pwd":  password,
		"json_data": string(jsonData),
	}
	formData := make([]byte, 0)
	for k, v := range form {
		formData = append(formData, []byte(k+"="+v+"&")...)
	}
	if len(formData) > 0 {
		formData = formData[:len(formData)-1]
	}
	// Custom HTTP client with InsecureSkipVerify
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}
	req, err := http.NewRequest("POST", baseURL, bytes.NewReader(formData))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	var result struct {
		Objects map[string]struct {
			Fields struct {
				Date string `json:"date"`
			} `json:"fields"`
		} `json:"objects"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	var dates []string
	for _, obj := range result.Objects {
		dates = append(dates, obj.Fields.Date)
	}
	return dates, nil
}
