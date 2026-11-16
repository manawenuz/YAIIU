package main

import (
	"errors"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"immich-proxy/internal/photobackup"
)

func main() {
	cfg, err := photobackup.ConfigFromEnv()
	if err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}
	store, err := photobackup.NewStore(cfg.DataDir, cfg.MaxUploadBytes)
	if err != nil {
		log.Fatalf("Could not initialize photo archive: %v", err)
	}
	app, err := newPhotoBackupServer(cfg, store)
	if err != nil {
		log.Fatalf("Could not initialize server: %v", err)
	}
	app.startForwardWorker()

	mode := "standalone archive"
	if cfg.ImmichServerURL != "" {
		mode = "archive + Immich forwarding"
	}
	log.Printf("Starting YAIIU photo server in %s mode", mode)
	log.Printf("Listening on %s; archive directory: %s", cfg.ListenAddr, cfg.DataDir)

	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           logMiddleware(app),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    1 << 20,
	}
	if cfg.TLSCertFile != "" {
		err = server.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
	} else {
		err = server.ListenAndServe()
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("Server stopped: %v", err)
		os.Exit(1)
	}
}

func logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" && r.URL.Path != "/healthz" {
			log.Printf("[%s] %s %s", GetClientIP(r), r.Method, r.URL.Path)
		}
		next.ServeHTTP(w, r)
	})
}

func GetClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if clientIP := strings.TrimSpace(strings.Split(xff, ",")[0]); clientIP != "" {
			return clientIP
		}
	}
	if value := r.Header.Get("X-Real-IP"); value != "" {
		return value
	}
	if value := r.Header.Get("CF-Connecting-IP"); value != "" {
		return value
	}
	return r.RemoteAddr
}
