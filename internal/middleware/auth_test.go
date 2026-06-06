package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func sentinel(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func TestBearerAuth_ValidToken(t *testing.T) {
	h := BearerAuth("secret", http.HandlerFunc(sentinel))
	req := httptest.NewRequest("GET", "/admin/engineers", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rr.Code)
	}
}

func TestBearerAuth_InvalidToken(t *testing.T) {
	h := BearerAuth("secret", http.HandlerFunc(sentinel))
	req := httptest.NewRequest("GET", "/admin/engineers", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rr.Code)
	}
}

func TestBearerAuth_MissingHeader(t *testing.T) {
	h := BearerAuth("secret", http.HandlerFunc(sentinel))
	req := httptest.NewRequest("GET", "/admin/engineers", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rr.Code)
	}
}

func TestBearerAuth_EmptyExpected_FailsClosed(t *testing.T) {
	// Empty token → 503 (misconfiguration guard)
	h := BearerAuth("", http.HandlerFunc(sentinel))
	req := httptest.NewRequest("GET", "/admin/engineers", nil)
	req.Header.Set("Authorization", "Bearer anything")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rr.Code)
	}
}

func TestBearerAuth_ConstantTimeCompare(t *testing.T) {
	// Token with same length but different content must be rejected
	h := BearerAuth("aaaa", http.HandlerFunc(sentinel))
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer bbbb")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("same-length wrong token: status = %d, want 401", rr.Code)
	}
}

func TestBearerAuth_RawTokenWithoutPrefix(t *testing.T) {
	// TrimPrefix is a convenience — raw token without "Bearer " still authenticates
	// because the comparison is on the stripped value.
	h := BearerAuth("secret", http.HandlerFunc(sentinel))
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "secret")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("raw token without prefix: status = %d, want 200", rr.Code)
	}
}
