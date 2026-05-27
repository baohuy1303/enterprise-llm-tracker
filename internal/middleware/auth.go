package middleware

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// BearerAuth wraps a handler with constant-time bearer-token comparison.
// If `expected` is empty, the middleware fails closed — every request gets
// 503, so misconfiguration can't silently expose admin routes.
func BearerAuth(expected string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if expected == "" {
			http.Error(w, "admin auth not configured", http.StatusServiceUnavailable)
			return
		}
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(got), []byte(expected)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
