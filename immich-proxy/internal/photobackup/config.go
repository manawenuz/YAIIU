package photobackup

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	ListenAddr         string
	DataDir            string
	Token              string
	StandaloneEmail    string
	StandaloneName     string
	StandalonePassword string
	ImmichServerURL    string
	ImmichAPIKey       string
	PublicBaseURL      string
	MaxUploadBytes     int64
	TLSCertFile        string
	TLSKeyFile         string
}

func ConfigFromEnv() (Config, error) {
	cfg := Config{
		ListenAddr:         envOr("LISTEN_ADDR", ":8080"),
		DataDir:            envOr("DATA_DIR", "./data"),
		Token:              strings.TrimSpace(os.Getenv("STANDALONE_API_KEY")),
		StandaloneEmail:    strings.TrimSpace(os.Getenv("STANDALONE_EMAIL")),
		StandaloneName:     envOr("STANDALONE_NAME", "Photo Backup"),
		StandalonePassword: os.Getenv("STANDALONE_PASSWORD"),
		ImmichServerURL:    strings.TrimRight(strings.TrimSpace(os.Getenv("IMMICH_SERVER_URL")), "/"),
		ImmichAPIKey:       strings.TrimSpace(os.Getenv("IMMICH_API_KEY")),
		PublicBaseURL:      strings.TrimRight(strings.TrimSpace(os.Getenv("PHOTO_BACKUP_PUBLIC_BASE_URL")), "/"),
		MaxUploadBytes:     100 * 1024 * 1024 * 1024,
		TLSCertFile:        strings.TrimSpace(os.Getenv("PHOTO_BACKUP_TLS_CERT_FILE")),
		TLSKeyFile:         strings.TrimSpace(os.Getenv("PHOTO_BACKUP_TLS_KEY_FILE")),
	}

	if raw := strings.TrimSpace(os.Getenv("MAX_UPLOAD_BYTES")); raw != "" {
		maxBytes, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || maxBytes < 1 {
			return Config{}, fmt.Errorf("MAX_UPLOAD_BYTES must be a positive integer")
		}
		cfg.MaxUploadBytes = maxBytes
	} else if raw := strings.TrimSpace(os.Getenv("PHOTO_BACKUP_MAX_UPLOAD_BYTES")); raw != "" {
		maxBytes, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || maxBytes < 1 {
			return Config{}, fmt.Errorf("PHOTO_BACKUP_MAX_UPLOAD_BYTES must be a positive integer")
		}
		cfg.MaxUploadBytes = maxBytes
	}

	if cfg.Token != "" && len(cfg.Token) < 32 {
		return Config{}, fmt.Errorf("STANDALONE_API_KEY must be at least 32 characters")
	}
	if cfg.ImmichAPIKey != "" && len(cfg.ImmichAPIKey) < 16 {
		return Config{}, fmt.Errorf("IMMICH_API_KEY must be at least 16 characters when set")
	}
	if cfg.ImmichServerURL == "" {
		if cfg.Token == "" {
			return Config{}, fmt.Errorf("STANDALONE_API_KEY is required when IMMICH_SERVER_URL is empty")
		}
		if cfg.StandaloneEmail == "" || cfg.StandalonePassword == "" {
			return Config{}, fmt.Errorf("STANDALONE_EMAIL and STANDALONE_PASSWORD are required when IMMICH_SERVER_URL is empty")
		}
		if len(cfg.StandalonePassword) < 12 {
			return Config{}, fmt.Errorf("STANDALONE_PASSWORD must be at least 12 characters")
		}
	}
	if cfg.ImmichServerURL != "" {
		u, err := url.Parse(cfg.ImmichServerURL)
		if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return Config{}, fmt.Errorf("IMMICH_SERVER_URL must be an HTTP(S) origin such as http://immich-server:2283")
		}
	}
	if cfg.PublicBaseURL != "" {
		u, err := url.Parse(cfg.PublicBaseURL)
		if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
			return Config{}, fmt.Errorf("PHOTO_BACKUP_PUBLIC_BASE_URL must be an origin such as https://photos.example.com")
		}
	}
	if (cfg.TLSCertFile == "") != (cfg.TLSKeyFile == "") {
		return Config{}, fmt.Errorf("PHOTO_BACKUP_TLS_CERT_FILE and PHOTO_BACKUP_TLS_KEY_FILE must be set together")
	}
	if strings.TrimSpace(cfg.DataDir) == "" {
		return Config{}, fmt.Errorf("DATA_DIR cannot be empty")
	}
	return cfg, nil
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
