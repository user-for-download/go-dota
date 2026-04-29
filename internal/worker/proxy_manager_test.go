package worker

import (
	"net/url"
	"testing"
)

func TestDedupMerge(t *testing.T) {
	tests := []struct {
		name     string
		a        []string
		b        []string
		want     []string
	}{
		{
			name:     "empty both",
			a:        []string{},
			b:        []string{},
			want:     []string{},
		},
		{
			name:     "a only",
			a:        []string{"proxy1", "proxy2"},
			b:        []string{},
			want:     []string{"proxy1", "proxy2"},
		},
		{
			name:     "b only",
			a:        []string{},
			b:        []string{"proxy1", "proxy2"},
			want:     []string{"proxy1", "proxy2"},
		},
		{
			name:     "merge no duplicates",
			a:        []string{"proxy1", "proxy2"},
			b:        []string{"proxy3", "proxy4"},
			want:     []string{"proxy1", "proxy2", "proxy3", "proxy4"},
		},
		{
			name:     "a first then b merge",
			a:        []string{"proxy1", "proxy2"},
			b:        []string{"proxy2", "proxy3"},
			want:     []string{"proxy1", "proxy2", "proxy3"},
		},
		{
			name:     "b duplicates",
			a:        []string{"proxy1"},
			b:        []string{"proxy1", "proxy1", "proxy2"},
			want:     []string{"proxy1", "proxy2"},
		},
		{
			name:     "empty strings ignored",
			a:        []string{"", "proxy1"},
			b:        []string{"proxy2", ""},
			want:     []string{"proxy1", "proxy2"},
		},
		{
			name:     "order preserved a first",
			a:        []string{"proxy1", "proxy2"},
			b:        []string{"proxy1", "proxy3"},
			want:     []string{"proxy1", "proxy2", "proxy3"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := dedupMerge(tc.a, tc.b)
			if len(got) != len(tc.want) {
				t.Errorf("dedupMerge() len = %d, want %d", len(got), len(tc.want))
				return
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("dedupMerge()[%d] = %s, want %s", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestIsValidProxyScheme(t *testing.T) {
	tests := []struct {
		name    string
		proxy   string
		want    bool
	}{
		{"http", "http://proxy.example.com:8080", true},
		{"https", "https://proxy.example.com:8080", true},
		{"socks5", "socks5://proxy.example.com:1080", true},
		{"uppercase SOCKS5", "SOCKS5://proxy.example.com", true},
		{"socks4 invalid", "socks4://proxy.example.com:1080", false},
		{"ftp invalid", "ftp://proxy.example.com", false},
		{"ip:port format", "proxy.example.com:8080", true},
		{"ip:port without port", "proxy.example.com", false},
		{"empty string", "", false},
		{"no scheme just host", "http://proxy.example.com", true},
		{"https without port", "https://proxy.example.com", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isValidProxyScheme(tc.proxy)
			if got != tc.want {
				t.Errorf("isValidProxyScheme(%q) = %v, want %v", tc.proxy, got, tc.want)
			}
		})
	}
}

func TestIsValidProxyScheme_URLParsing(t *testing.T) {
	// Test edge cases related to URL parsing
	tests := []struct {
		name  string
		input string
	}{
		{"with path", "http://proxy.example.com:8080/path"},
		{"with query", "http://proxy.example.com:8080?foo=bar"},
		{"with credentials", "http://user:pass@proxy.example.com:8080"},
		{"ipv4", "http://192.168.1.1:8080"},
		{"ipv6", "http://[::1]:8080"},
		{"localhost", "http://localhost:8080"},
		{"with port only", "http://proxy.example:8080"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			u, err := url.Parse(tc.input)
			if err != nil {
				t.Fatalf("url.Parse failed: %v", err)
			}
			_ = u
			// Just ensure these don't panic
		})
	}
}

func TestParseTextProxies(t *testing.T) {
	pm := &ProxyManager{}

	tests := []struct {
		name  string
		body  string
		want  []string
	}{
		{
			name:  "empty",
			body:  "",
			want:  []string{},
		},
		{
			name:  "single proxy",
			body:  "http://proxy1.example.com:8080",
			want:  []string{"http://proxy1.example.com:8080"},
		},
		{
			name:  "multiple lines",
			body:  "http://proxy1.example.com:8080\nhttp://proxy2.example.com:8080\nhttp://proxy3.example.com:8080",
			want:  []string{"http://proxy1.example.com:8080", "http://proxy2.example.com:8080", "http://proxy3.example.com:8080"},
		},
		{
			name:  "no scheme added",
			body:  "proxy.example.com:8080",
			want:  []string{"http://proxy.example.com:8080"},
		},
		{
			name:  "socks5 without scheme",
			body:  "socks5://proxy.example.com:1080",
			want:  []string{"socks5://proxy.example.com:1080"},
		},
		{
			name:  "skip json",
			body:  `{"proxies": ["http://proxy1.com"]}`,
			want:  []string{},
		},
		{
			name:  "skip array",
			body:  `["http://proxy1.com"]`,
			want:  []string{},
		},
		{
			name:  "skip empty lines",
			body:  "http://proxy1.com\n\nhttp://proxy2.com\n",
			want:  []string{"http://proxy1.com", "http://proxy2.com"},
		},
		{
			name:  "mixed valid and invalid",
			body:  "http://good.com\nbad\nhttp://also-good.com",
			want:  []string{"http://good.com", "http://bad", "http://also-good.com"},
		},
		{
			name:  "socks4 rejected",
			body:  "socks4://proxy.example.com",
			want:  []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := pm.parseTextProxies([]byte(tc.body))
			if len(got) != len(tc.want) {
				t.Errorf("parseTextProxies() len = %d, want %d", len(got), len(tc.want))
				return
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("parseTextProxies()[%d] = %s, want %s", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestParseLocalProxies(t *testing.T) {
	pm := &ProxyManager{}

	tests := []struct {
		name    string
		data    string
		want    []string
		wantErr bool
	}{
		{
			name:    "valid json alive true",
			data:    `{"proxies": [{"alive": true, "proxy": "http://proxy1.com:8080"}]}`,
			want:    []string{"http://proxy1.com:8080"},
			wantErr: false,
		},
		{
			name:    "valid json alive false",
			data:    `{"proxies": [{"alive": false, "proxy": "http://proxy1.com:8080"}]}`,
			want:    []string{},
			wantErr: false,
		},
		{
			name:    "valid json multiple",
			data:    `{"proxies": [{"alive": true, "proxy": "http://p1.com"}, {"alive": true, "proxy": "http://p2.com"}]}`,
			want:    []string{"http://p1.com", "http://p2.com"},
			wantErr: false,
		},
		{
			name:    "no scheme adds http",
			data:    `{"proxies": [{"alive": true, "proxy": "proxy.example.com:8080"}]}`,
			want:    []string{"http://proxy.example.com:8080"},
			wantErr: false,
		},
		{
			name:    "invalid json",
			data:    `{invalid}`,
			want:    nil,
			wantErr: true,
		},
		{
			name:    "empty proxies",
			data:    `{"proxies": []}`,
			want:    []string{},
			wantErr: false,
		},
		{
			name:    "empty proxy string",
			data:    `{"proxies": [{"alive": true, "proxy": ""}]}`,
			want:    []string{},
			wantErr: false,
		},
		{
			name:    "socks5 preserved",
			data:    `{"proxies": [{"alive": true, "proxy": "socks5://proxy.com"}]}`,
			want:    []string{"socks5://proxy.com"},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := pm.parseLocalProxies([]byte(tc.data))
			if tc.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Errorf("parseLocalProxies() len = %d, want %d", len(got), len(tc.want))
				return
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("parseLocalProxies()[%d] = %s, want %s", i, got[i], tc.want[i])
				}
			}
		})
	}
}