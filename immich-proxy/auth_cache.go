package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const validatedTokenTTL = 30 * 24 * time.Hour

type validatedTokenCache struct {
	mu     sync.Mutex
	path   string
	tokens map[string]time.Time
}

func newValidatedTokenCache(dataDir string) (*validatedTokenCache, error) {
	root, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("resolve token cache directory: %w", err)
	}
	cache := &validatedTokenCache{
		path:   filepath.Join(root, ".validated-auth-tokens.json"),
		tokens: make(map[string]time.Time),
	}
	data, err := os.ReadFile(cache.path)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("read validated token cache: %w", err)
	}
	if err == nil {
		if err := json.Unmarshal(data, &cache.tokens); err != nil {
			return nil, fmt.Errorf("parse validated token cache: %w", err)
		}
	}
	cache.pruneLocked(time.Now())
	return cache, nil
}

func (c *validatedTokenCache) Remember(token string) error {
	if token == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneLocked(time.Now())
	c.tokens[hashToken(token)] = time.Now().Add(validatedTokenTTL)
	return c.persistLocked()
}

func (c *validatedTokenCache) Contains(token string) bool {
	if token == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	expiresAt, ok := c.tokens[hashToken(token)]
	return ok && expiresAt.After(time.Now())
}

func (c *validatedTokenCache) pruneLocked(now time.Time) {
	for tokenHash, expiresAt := range c.tokens {
		if !expiresAt.After(now) {
			delete(c.tokens, tokenHash)
		}
	}
}

func (c *validatedTokenCache) persistLocked() error {
	data, err := json.Marshal(c.tokens)
	if err != nil {
		return err
	}
	dir := filepath.Dir(c.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	file, err := os.CreateTemp(dir, ".token-cache-*")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(name, c.path)
}

func hashToken(token string) string {
	hash := sha256.Sum256([]byte(token))
	return hex.EncodeToString(hash[:])
}
