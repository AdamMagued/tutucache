package main

import "testing"

func TestListenAddrFromURL(t *testing.T) {
	tests := []struct {
		name    string
		rawURL  string
		want    string
		wantErr bool
	}{
		{name: "http", rawURL: "http://localhost:8001", want: "localhost:8001"},
		{name: "https", rawURL: "https://localhost:8001", want: "localhost:8001"},
		{name: "ipv4", rawURL: "http://127.0.0.1:8001", want: "127.0.0.1:8001"},
		{name: "ipv6", rawURL: "http://[::1]:8001", want: "[::1]:8001"},
		{name: "path is ignored", rawURL: "http://localhost:8001/geecache/", want: "localhost:8001"},
		{name: "missing scheme", rawURL: "localhost:8001", wantErr: true},
		{name: "missing port", rawURL: "https://localhost", wantErr: true},
		{name: "missing host", rawURL: "http://", wantErr: true},
	}

	for _, tt := range tests {
		got, err := listenAddrFromURL(tt.rawURL)
		if tt.wantErr {
			if err == nil {
				t.Errorf("%s: listenAddrFromURL(%q) = %q, want error", tt.name, tt.rawURL, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: listenAddrFromURL(%q) error: %v", tt.name, tt.rawURL, err)
			continue
		}
		if got != tt.want {
			t.Errorf("%s: listenAddrFromURL(%q) = %q, want %q", tt.name, tt.rawURL, got, tt.want)
		}
	}
}
