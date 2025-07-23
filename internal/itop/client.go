package itop

import (
	"crypto/tls"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
)

type ITopClient struct {
	BaseURL  string
	Username string
	Password string
	Version  string
}

func (c *ITopClient) Post(operation string, params map[string]interface{}) ([]byte, error) {
	params["operation"] = operation
	jsonData, _ := json.Marshal(params)

	form := url.Values{}
	form.Set("version", c.Version)
	form.Set("auth_user", c.Username)
	form.Set("auth_pwd", c.Password)
	form.Set("json_data", string(jsonData))

	req, err := http.NewRequest("POST", c.BaseURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		log.Printf("iTop API response status: %d", resp.StatusCode)
		log.Printf("iTop API response body: %s", string(body))
		return nil, err
	}
	return body, err
}
