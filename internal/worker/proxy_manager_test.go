package worker

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestNewProxyManager(t *testing.T) {
	pm := NewProxyManager(
		nil,
		"https://api.example.com/proxies",
		"https://httpbin.org/ip",
		15*time.Second,
		nil,
	)

	if pm.proxyProviderURL != "https://api.example.com/proxies" {
		t.Errorf("proxyProviderURL = %s, want %s",
			pm.proxyProviderURL, "https://api.example.com/proxies")
	}
	if pm.healthCheckURL != "https://httpbin.org/ip" {
		t.Errorf("healthCheckURL = %s, want %s",
			pm.healthCheckURL, "https://httpbin.org/ip")
	}
	if pm.refreshInterval != 15*time.Second {
		t.Errorf("refreshInterval = %v, want %v", pm.refreshInterval, 15*time.Second)
	}
}

func TestParseTextProxies(t *testing.T) {
	pm := NewProxyManager(nil, "", "", 0, nil)

	tests := []struct {
		name string
		body []byte
		want int
	}{
		{"empty body", []byte(""), 0},
		{"blank lines only", []byte("\n\n\n"), 0},
		{"mixed blank and valid", []byte("http://1.2.3.4:80\n\nhttp://5.6.7.8:8080"), 2},
		{"bare IPs get http prefix", []byte("1.2.3.4:80\n5.6.7.8:8080"), 2},
		{"already prefixed", []byte("socks5://127.0.0.1:1080\nhttp://127.0.0.1:8080"), 2},
		{"with whitespace", []byte("  http://1.2.3.4:80  \n  \n  http://5.6.7.8:8080  "), 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := pm.parseTextProxies(tc.body)
			if len(got) != tc.want {
				t.Errorf("parseTextProxies() len = %d, want %d; got = %v",
					len(got), tc.want, got)
			}
		})
	}
}

func TestParseLocalProxies(t *testing.T) {
	pm := NewProxyManager(nil, "", "", 0, nil)

	tests := []struct {
		name string
		data []byte
		want int
	}{
		{
			"alive only",
			[]byte(`{"proxies":[{"alive":true,"proxy":"http://1.1.1.1:80"},{"alive":false,"proxy":"http://2.2.2.2:80"}]}`),
			1,
		},
		{
			"empty proxy string skipped",
			[]byte(`{"proxies":[{"alive":true,"proxy":""},{"alive":true,"proxy":"http://3.3.3.3:80"}]}`),
			1,
		},
		{
			"all alive",
			[]byte(`{"proxies":[{"alive":true,"proxy":"http://1.1.1.1:80"},{"alive":true,"proxy":"http://2.2.2.2:80"},{"alive":true,"proxy":"http://3.3.3.3:80"}]}`),
			3,
		},
		{
			"all dead",
			[]byte(`{"proxies":[{"alive":false,"proxy":"http://1.1.1.1:80"},{"alive":false,"proxy":"http://2.2.2.2:80"}]}`),
			0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := pm.parseLocalProxies(tc.data)
			if err != nil {
				t.Fatalf("parseLocalProxies() error = %v", err)
			}
			if len(got) != tc.want {
				t.Errorf("parseLocalProxies() len = %d, want %d", len(got), tc.want)
			}
		})
	}
}

func TestReadLocalProxies(t *testing.T) {
	pm := NewProxyManager(nil, "", "", 0, nil)

	tmpFile, err := os.CreateTemp("", "proxy_test.json")
	if err != nil {
		t.Fatalf("CreateTemp() error = %v", err)
	}
	defer os.Remove(tmpFile.Name())

	jsonData := `{"proxies":[{"alive":true,"proxy":"socks5://192.168.1.1:1080"},{"alive":true,"proxy":"http://10.0.0.1:8080"}]}`
	if _, err := tmpFile.WriteString(jsonData); err != nil {
		t.Fatalf("WriteString() error = %v", err)
	}
	tmpFile.Close()

	pm.localProxyFile = tmpFile.Name()

	proxies, err := pm.readLocalProxies(context.Background())
	if err != nil {
		t.Fatalf("readLocalProxies() error = %v", err)
	}
	if len(proxies) != 2 {
		t.Errorf("readLocalProxies() len = %d, want 2", len(proxies))
	}
	if proxies[0] != "socks5://192.168.1.1:1080" {
		t.Errorf("proxies[0] = %s, want socks5://192.168.1.1:1080", proxies[0])
	}
}

func TestDedupMerge(t *testing.T) {
	a := []string{"http://1", "http://2"}
	b := []string{"http://2", "http://3"}

	got := dedupMerge(a, b)

	want := []string{"http://1", "http://2", "http://3"}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("index %d = %s, want %s", i, got[i], want[i])
		}
	}
}
