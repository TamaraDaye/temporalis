package gateway

import (
	"testing"
	"encoding/json"
)


func TestConfig(t *testing.T) {
	path := "./gateway-example.yaml"
	config, err := Load(path)

	if err != nil {
		t.Logf("failed to load config %+v", err)
	}


	b, _ := json.MarshalIndent(config, "", "  ")

	t.Logf("this is the config \n%s", b)
}

