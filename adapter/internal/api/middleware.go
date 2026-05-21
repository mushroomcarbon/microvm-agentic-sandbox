package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log"
	"net/http"
	"time"
)

// Middleware wraps an http.Handler.
type Middleware func(http.Handler) http.Handler

// Chain composes middlewares around h. The first middleware in the list ends
// up outermost, so it sees the request first and the response last.
func Chain(h http.Handler, mws ...Middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// ctxKey is a private type to avoid collisions in context value keys.
type ctxKey string

const (
	ctxKeyRequestID ctxKey = "request_id"
	ctxKeyTenantID  ctxKey = "tenant_id"
	ctxKeyOperator  ctxKey = "operator"

)

// RequestIDMiddleware reads X-Request-ID (generating one if absent) and
// threads it through the request context, also echoing it back on the response
// header so clients can correlate without tracing infra.
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			reqID = newRequestID()
		}
		ctx := context.WithValue(r.Context(), ctxKeyRequestID, reqID)
		w.Header().Set("X-Request-ID", reqID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// TenantHeadersMiddleware extracts identity headers and puts them on the
// context. No enforcement at this layer — callers are trusted.
func TenantHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if v := r.Header.Get("X-Tenant-ID"); v != "" {
			ctx = context.WithValue(ctx, ctxKeyTenantID, v)
		}
		if v := r.Header.Get("X-Operator"); v != "" {
			ctx = context.WithValue(ctx, ctxKeyOperator, v)
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// LoggingMiddleware logs method, path, status, and duration with the request id.
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		reqID, _ := r.Context().Value(ctxKeyRequestID).(string)
		log.Printf("req=%s %s %s -> %d (%s)", reqID, r.Method, r.URL.Path, rw.status, time.Since(start))
	})
}

// RecoverMiddleware turns handler panics into 500 responses so a bug in one
// handler doesn't take down the process.
func RecoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("panic: %v", rec)
				writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// statusRecorder captures the status code so the logger can see it after the
// handler returns. Also delegates http.Flusher so streaming handlers (SSE)
// can still flush through the middleware wrapper.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (sr *statusRecorder) WriteHeader(code int) {
	sr.status = code
	sr.ResponseWriter.WriteHeader(code)
}

// Flush forwards to the underlying ResponseWriter if it supports it, which
// stdlib's *http.response always does for HTTP/1.x and HTTP/2 connections.
// Required for SSE handlers that need to push events as they're produced.
func (sr *statusRecorder) Flush() {
	if f, ok := sr.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// newRequestID returns a 16-byte random hex string for callers that didn't
// supply X-Request-ID.
func newRequestID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Accessors so handlers don't need to know the ctxKey type.

func RequestIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyRequestID).(string)
	return v
}

func TenantIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyTenantID).(string)
	return v
}

func OperatorFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyOperator).(string)
	return v
}