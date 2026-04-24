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
		t.Errorf("proxyProviderURL = %s, want %s", pm.proxyProviderURL, "https://api.example.com/proxies")
	}

	if pm.healthCheckURL != "https://httpbin.org/ip" {
		t.Errorf("healthCheckURL = %s, want %s", pm.healthCheckURL, "https://httpbin.org/ip")
	}

	if pm.refreshInterval != 15*time.Second {
		t.Errorf("refreshInterval = %v, want %v", pm.refreshInterval, 15*time.Second)
	}
}

func TestProxyManagerFields(t *testing.T) {
	pm := NewProxyManager(nil, "test-url", "health-url", 30, nil)

	if pm.proxyProviderURL != "test-url" {
		t.Errorf("proxyProviderURL = %s, want test-url", pm.proxyProviderURL)
	}

	if pm.healthCheckURL != "health-url" {
		t.Errorf("healthCheckURL = %s, want health-url", pm.healthCheckURL)
	}

	if pm.refreshInterval != 30 {
		t.Errorf("refreshInterval = %d, want 30", pm.refreshInterval)
	}
}

func TestParseTextProxies(t *testing.T) {
	pm := NewProxyManager(nil, "", "", 0, nil)

	tests := []struct {
		name  string
		body  []byte
		want  int
	}{
		{
			name:  "empty body",
			body:  []byte(""),
			want:  0,
		},
		{
			name:  "blank lines only",
			body:  []byte("\n\n\n"),
			want:  0,
		},
		{
			name:  "mixed blank and valid",
			body:  []byte("http://1.2.3.4:80\n\nhttp://5.6.7.8:8080"),
			want:  2,
		},
		{
			name:  "bare IPs get http prefix",
			body:  []byte("1.2.3.4:80\n5.6.7.8:8080"),
			want:  2,
		},
		{
			name:  "already prefixed",
			body:  []byte("socks5://127.0.0.1:1080\nhttp://127.0.0.1:8080"),
			want:  2,
		},
		{
			name:  "with whitespace",
			body:  []byte("  http://1.2.3.4:80  \n  \n  http://5.6.7.8:8080  "),
			want:  2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := pm.parseTextProxies(tc.body)
			if len(got) != tc.want {
				t.Errorf("parseTextProxies() len = %d, want %d; got = %v", len(got), tc.want, got)
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
			name: "alive only",
			data: []byte(`{"proxies":[{"alive":true,"proxy":"http://1.1.1.1:80"},{"alive":false,"proxy":"http://2.2.2.2:80"}]}`),
			want: 1,
		},
		{
			name: "empty proxy string skipped",
			data: []byte(`{"proxies":[{"alive":true,"proxy":""},{"alive":true,"proxy":"http://3.3.3.3:80"}]}`),
			want: 1,
		},
		{
			name: "all alive",
			data: []byte(`{"proxies":[{"alive":true,"proxy":"http://1.1.1.1:80"},{"alive":true,"proxy":"http://2.2.2.2:80"},{"alive":true,"proxy":"http://3.3.3.3:80"}]}`),
			want: 3,
		},
		{
			name: "all dead",
			data: []byte(`{"proxies":[{"alive":false,"proxy":"http://1.1.1.1:80"},{"alive":false,"proxy":"http://2.2.2.2:80"}]}`),
			want: 0,
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

func TestReadLocalProxiesHappyPath(t *testing.T) {
	pm := NewProxyManager(nil, "", "", 0, nil)

	tmpFile, err := os.CreateTemp("", "proxy.json")
	if err != nil {
		t.Fatalf("CreateTemp() error = %v", err)
	}
	defer os.Remove(tmpFile.Name())

	jsonData := `{"proxies":[{"alive":true,"proxy":"socks5://192.168.1.1:1080"},{"alive":true,"proxy":"http://10.0.0.1:8080"},{"alive":false,"proxy":"http://dead.proxy:80"}]}`
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
	if proxies[1] != "http://10.0.0.1:8080" {
		t.Errorf("proxies[1] = %s, want http://10.0.0.1:8080", proxies[1])
	}
}

func TestReadLocalProxiesPrecedence(t *testing.T) {
	pm := NewProxyManager(nil, "", "", 0, nil)

	t.Run("empty localProxyFile returns nil", func(t *testing.T) {
		proxies, err := pm.readLocalProxies(context.Background())
		if err != nil {
			t.Errorf("readLocalProxies() unexpected error = %v", err)
		}
		if proxies != nil {
			t.Errorf("readLocalProxies() = %v, want nil", proxies)
		}
	})

	pm.localProxyFile = "/nonexistent/path/proxy.json"
	t.Run("nonexistent configured file returns nil", func(t *testing.T) {
		proxies, err := pm.readLocalProxies(context.Background())
		if err != nil {
			t.Errorf("readLocalProxies() unexpected error = %v", err)
		}
		if proxies != nil {
			t.Errorf("readLocalProxies() = %v, want nil", proxies)
		}
	})
}