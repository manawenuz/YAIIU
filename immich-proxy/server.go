package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httputil"
	"net/textproto"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"immich-proxy/handlers"
	"immich-proxy/internal/photobackup"
)

const (
	maxLoginBodyBytes = 4 << 10
	maxMetadataBytes  = 1 << 20
	maxResponseBytes  = 1 << 20
	standaloneUserID  = "00000000-0000-4000-8000-000000000001"
)

type photoBackupServer struct {
	cfg        photobackup.Config
	store      *photobackup.Store
	immich     *httputil.ReverseProxy
	immichURL  *url.URL
	httpClient *http.Client
	authTokens *validatedTokenCache
	forwardMu  sync.Mutex
	forwarding map[string]struct{}
}

func newPhotoBackupServer(cfg photobackup.Config, store *photobackup.Store) (*photoBackupServer, error) {
	s := &photoBackupServer{
		cfg:        cfg,
		store:      store,
		httpClient: &http.Client{},
		forwarding: make(map[string]struct{}),
	}
	if cfg.ImmichServerURL == "" {
		return s, nil
	}

	target, err := url.Parse(cfg.ImmichServerURL)
	if err != nil {
		return nil, fmt.Errorf("parse IMMICH_SERVER_URL: %w", err)
	}
	s.immichURL = target
	s.immich = httputil.NewSingleHostReverseProxy(target)
	originalDirector := s.immich.Director
	s.immich.Director = func(req *http.Request) {
		originalDirector(req)
		if req.URL.Path == "/api/auth/login" {
			req.Header.Set("Accept-Encoding", "identity")
		}
	}
	s.authTokens, err = newValidatedTokenCache(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	s.immich.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		writeAPIError(w, http.StatusBadGateway, "Immich is unavailable")
	}
	s.immich.ModifyResponse = func(resp *http.Response) error {
		if resp.Request == nil || resp.Request.URL.Path != "/api/auth/login" || resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, maxLoginBodyBytes+1))
		if err != nil {
			return err
		}
		if len(body) > maxLoginBodyBytes {
			return fmt.Errorf("Immich login response exceeded size limit")
		}
		_ = resp.Body.Close()
		resp.Body = io.NopCloser(bytes.NewReader(body))
		var result struct {
			AccessToken string `json:"accessToken"`
		}
		if json.Unmarshal(body, &result) == nil && result.AccessToken != "" {
			if err := s.authTokens.Remember(result.AccessToken); err != nil {
				log.Printf("Could not persist a validated Immich token hash: %v", err)
			}
		}
		return nil
	}
	return s, nil
}

func (s *photoBackupServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/health" || r.URL.Path == "/healthz" {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, http.StatusOK, map[string]string{
			"status": "ok",
			"time":   time.Now().UTC().Format(time.RFC3339),
		})
		return
	}

	if r.URL.Path == "/api/archive/assets" || strings.HasPrefix(r.URL.Path, "/api/archive/assets/") {
		s.handleArchiveAPI(w, r)
		return
	}

	if r.URL.Path == "/api/auth/login" && s.immich == nil {
		s.handleStandaloneLogin(w, r)
		return
	}

	if r.URL.Path == "/api/assets/background" {
		s.handleBackgroundUpload(w, r)
		return
	}
	if r.URL.Path == "/api/assets" && r.Method == http.MethodPost {
		s.handleMultipartUpload(w, r)
		return
	}

	if s.immich != nil {
		s.immich.ServeHTTP(w, r)
		return
	}
	s.serveStandalone(w, r)
}

func (s *photoBackupServer) handleStandaloneLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxLoginBodyBytes)
	var credentials struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&credentials); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid login request")
		return
	}
	if !strings.EqualFold(strings.TrimSpace(credentials.Email), strings.TrimSpace(s.cfg.StandaloneEmail)) ||
		subtle.ConstantTimeCompare([]byte(credentials.Password), []byte(s.cfg.StandalonePassword)) != 1 {
		writeAPIError(w, http.StatusUnauthorized, "invalid email or password")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]string{
		"accessToken": s.cfg.Token,
		"userId":      standaloneUserID,
		"userEmail":   s.cfg.StandaloneEmail,
		"name":        s.cfg.StandaloneName,
	})
}

func (s *photoBackupServer) serveStandalone(w http.ResponseWriter, r *http.Request) {
	if !s.validStandaloneBearer(r) {
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeAPIError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	switch {
	case r.URL.Path == "/api/users/me" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]string{
			"id":    standaloneUserID,
			"email": s.cfg.StandaloneEmail,
			"name":  s.cfg.StandaloneName,
		})
	case r.URL.Path == "/api/sync/stream" && r.Method == http.MethodPost:
		// YAIIU uses Immich's incremental stream to reconcile server assets. The
		// standalone archive has no remote library, so a successful empty stream
		// tells the app there is nothing to reconcile.
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
	case r.URL.Path == "/api/sync/ack" && r.Method == http.MethodPost:
		w.WriteHeader(http.StatusNoContent)
	case r.URL.Path == "/api/albums" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, []any{})
	case r.URL.Path == "/api/partners" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, []any{})
	default:
		writeAPIError(w, http.StatusNotFound, "endpoint is unavailable in standalone archive mode")
	}
}

func (s *photoBackupServer) handleBackgroundUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		// YAIIU's PhotoKit extension uses 501 to select its non-resumable upload
		// path. Keep the existing protocol behavior.
		w.WriteHeader(http.StatusNotImplemented)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "OPTIONS, POST")
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.authorizeUpload(w, r) {
		return
	}
	if r.ContentLength > s.cfg.MaxUploadBytes {
		writeAPIError(w, http.StatusRequestEntityTooLarge, "upload exceeds configured size limit")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxUploadBytes)
	metadata := s.backgroundMetadata(r)
	asset, duplicate, err := s.store.SaveUpload(metadata, r.Body)
	if err != nil {
		writeUploadError(w, err)
		return
	}
	s.respondStoredUpload(w, r, asset, duplicate)
}

func (s *photoBackupServer) handleMultipartUpload(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeUpload(w, r) {
		return
	}
	requestLimit := multipartRequestLimit(s.cfg.MaxUploadBytes)
	if r.ContentLength > requestLimit {
		writeAPIError(w, http.StatusRequestEntityTooLarge, "upload exceeds configured size limit")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, requestLimit)
	if err := r.ParseMultipartForm(1 << 20); err != nil {
		if r.MultipartForm != nil {
			_ = r.MultipartForm.RemoveAll()
		}
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			writeAPIError(w, http.StatusRequestEntityTooLarge, "upload exceeds configured size limit")
		} else {
			writeAPIError(w, http.StatusBadRequest, "invalid multipart upload")
		}
		return
	}
	if r.MultipartForm != nil {
		defer r.MultipartForm.RemoveAll()
	}
	file, fileHeader, err := r.FormFile("assetData")
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "multipart upload must include assetData")
		return
	}
	defer file.Close()
	metadata := s.multipartMetadata(r, fileHeader)
	asset, duplicate, err := s.store.SaveUpload(metadata, file)
	if err != nil {
		writeUploadError(w, err)
		return
	}
	s.respondStoredUpload(w, r, asset, duplicate)
}

func (s *photoBackupServer) authorizeUpload(w http.ResponseWriter, r *http.Request) bool {
	if s.immich == nil {
		if s.validStandaloneBearer(r) {
			return true
		}
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeAPIError(w, http.StatusUnauthorized, "unauthorized")
		return false
	}
	if !hasClientAuth(r) {
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeAPIError(w, http.StatusUnauthorized, "Immich authentication is required")
		return false
	}
	credential := clientCredential(r)
	if s.authTokens.Contains(credential) {
		return true
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, s.immichURL.String()+"/api/users/me", nil)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "could not validate Immich credentials")
		return false
	}
	copyClientAuth(req.Header, r.Header)
	resp, err := s.httpClient.Do(req)
	if err != nil {
		if s.authTokens.Contains(credential) {
			return true
		}
		writeAPIError(w, http.StatusBadGateway, "Immich is unavailable")
		return false
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		if err := s.authTokens.Remember(credential); err != nil {
			log.Printf("Could not persist a validated Immich token hash: %v", err)
		}
		return true
	}
	if resp.StatusCode >= http.StatusInternalServerError && s.authTokens.Contains(credential) {
		return true
	}
	if resp.StatusCode >= http.StatusInternalServerError {
		writeAPIError(w, http.StatusBadGateway, "Immich is unavailable")
		return false
	}
	if resp.StatusCode != http.StatusOK {
		writeAPIError(w, http.StatusUnauthorized, "Immich rejected the supplied credentials")
		return false
	}
	return false
}

func (s *photoBackupServer) backgroundMetadata(r *http.Request) photobackup.UploadMetadata {
	filename := firstNonEmpty(r.Header.Get("X-Filename"), r.URL.Query().Get("filename"), "upload.bin")
	createdAt := parseTimestamp(firstNonEmpty(r.Header.Get("X-File-Created-At"), r.URL.Query().Get("fileCreatedAt")))
	modifiedAt := parseTimestamp(firstNonEmpty(r.Header.Get("X-File-Modified-At"), r.URL.Query().Get("fileModifiedAt")))
	deviceAssetID := firstNonEmpty(r.Header.Get("X-Device-Asset-Id"), r.URL.Query().Get("deviceAssetId"))
	if deviceAssetID == "" {
		deviceAssetID = fmt.Sprintf("%s-%d", filename, time.Now().UnixNano())
	}
	mediaType := firstNonEmpty(r.Header.Get("X-Content-Type"), mime.TypeByExtension(filepath.Ext(filename)), "application/octet-stream")
	return photobackup.UploadMetadata{
		DeviceID:       firstNonEmpty(r.Header.Get("X-Device-Id"), r.URL.Query().Get("deviceId"), "ios-yaiiu"),
		DeviceAssetID:  deviceAssetID,
		AssetVersion:   firstNonEmpty(r.Header.Get("X-YAIIU-Asset-Version"), r.URL.Query().Get("assetVersion")),
		ResourceType:   resourceType(mediaType),
		Filename:       filename,
		MediaType:      mediaType,
		CapturedAt:     createdAt,
		FileModifiedAt: firstNonZeroTime(modifiedAt, createdAt),
		IsFavorite:     parseBool(r.Header.Get("X-Is-Favorite")),
		ICloudID:       firstNonEmpty(r.Header.Get("X-iCloud-Id"), r.URL.Query().Get("iCloudId")),
		Latitude:       firstNonEmpty(r.Header.Get("X-Latitude"), r.URL.Query().Get("latitude")),
		Longitude:      firstNonEmpty(r.Header.Get("X-Longitude"), r.URL.Query().Get("longitude")),
		TimezoneOffset: firstNonEmpty(r.Header.Get("X-Timezone-Offset"), r.URL.Query().Get("timezoneOffset")),
	}
}

func (s *photoBackupServer) multipartMetadata(r *http.Request, fileHeader *multipart.FileHeader) photobackup.UploadMetadata {
	filename := firstNonEmpty(fileHeader.Filename, "upload.bin")
	mediaType := firstNonEmpty(fileHeader.Header.Get("Content-Type"), mime.TypeByExtension(filepath.Ext(filename)), "application/octet-stream")
	createdRaw := r.FormValue("fileCreatedAt")
	modifiedRaw := r.FormValue("fileModifiedAt")
	cloudID, latitude, longitude, checksum, metadataCreatedAt := parseMobileMetadata(r.FormValue("metadata"))
	createdAt := parseTimestamp(firstNonEmpty(createdRaw, metadataCreatedAt))
	modifiedAt := firstNonZeroTime(parseTimestamp(modifiedRaw), createdAt)
	deviceAssetID := r.FormValue("deviceAssetId")
	if deviceAssetID == "" {
		deviceAssetID = fmt.Sprintf("%s-%d", filename, time.Now().UnixNano())
	}
	return photobackup.UploadMetadata{
		DeviceID:       firstNonEmpty(r.FormValue("deviceId"), "ios-yaiiu"),
		DeviceAssetID:  deviceAssetID,
		ResourceType:   resourceType(mediaType),
		Filename:       filename,
		MediaType:      mediaType,
		CapturedAt:     createdAt,
		FileModifiedAt: modifiedAt,
		IsFavorite:     parseBool(r.FormValue("isFavorite")),
		ICloudID:       cloudID,
		Latitude:       latitude,
		Longitude:      longitude,
		SourceChecksum: checksum,
	}
}

func (s *photoBackupServer) respondStoredUpload(w http.ResponseWriter, r *http.Request, asset photobackup.Asset, duplicate bool) {
	if s.immich == nil {
		writeJSON(w, http.StatusCreated, map[string]any{"id": asset.ID, "duplicate": duplicate})
		return
	}
	if asset.ImmichAssetID != "" {
		w.Header().Set("X-YAIIU-Immich-Asset-Id", asset.ImmichAssetID)
		writeJSON(w, http.StatusCreated, map[string]any{"id": asset.ImmichAssetID, "duplicate": true})
		return
	}
	if !s.claimForward(asset.ID) {
		w.Header().Set("X-Photo-Backup-Stored", "true")
		writeAPIError(w, http.StatusBadGateway, "original is safely stored and forwarding is already in progress")
		return
	}
	defer s.releaseForward(asset.ID)
	asset, filePath, err := s.store.GetAsset(asset.ID)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "original was saved but could not be reopened for Immich")
		return
	}
	remoteID, remoteDuplicate, status, responseBody, err := s.forwardAsset(r.Context(), r.Header, asset, filePath)
	if err != nil {
		_ = s.store.MarkForwardResult(asset.ID, "", "Immich request failed")
		w.Header().Set("X-Photo-Backup-Stored", "true")
		writeAPIError(w, http.StatusBadGateway, "original is safely stored; forwarding to Immich failed")
		return
	}
	if status < http.StatusOK || status >= http.StatusMultipleChoices {
		_ = s.store.MarkForwardResult(asset.ID, "", fmt.Sprintf("Immich returned HTTP %d", status))
		w.Header().Set("X-Photo-Backup-Stored", "true")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(responseBody)
		return
	}
	if remoteID == "" {
		_ = s.store.MarkForwardResult(asset.ID, "", "Immich response did not include an asset id")
		w.Header().Set("X-Photo-Backup-Stored", "true")
		writeAPIError(w, http.StatusBadGateway, "original is safely stored; Immich returned no asset id")
		return
	}
	if err := s.store.MarkForwardResult(asset.ID, remoteID, ""); err != nil {
		writeAPIError(w, http.StatusInternalServerError, "Immich accepted the upload but its archive status could not be saved")
		return
	}
	w.Header().Set("X-YAIIU-Immich-Asset-Id", remoteID)
	if len(responseBody) == 0 {
		responseBody, _ = json.Marshal(map[string]any{"id": remoteID, "duplicate": duplicate || remoteDuplicate})
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(responseBody)
}

func (s *photoBackupServer) forwardAsset(ctx context.Context, clientHeaders http.Header, asset photobackup.Asset, filePath string) (string, bool, int, []byte, error) {
	if asset.SourceChecksum == "" && asset.TimezoneOffset != "" {
		normalizedPath, changed, err := handlers.AddTimezoneOffsetFileIfMissing(filePath, asset.Filename, asset.TimezoneOffset)
		if err != nil {
			log.Printf("Skipping server-side timezone normalization for %s: %v", asset.Filename, err)
		} else if changed {
			defer os.Remove(normalizedPath)
			filePath = normalizedPath
			asset.SourceChecksum = asset.SHA1
		}
	}
	file, err := os.Open(filePath)
	if err != nil {
		return "", false, 0, nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return "", false, 0, nil, err
	}
	metadata := handlers.BackgroundUploadRequest{
		DeviceAssetID:  asset.DeviceAssetID,
		DeviceID:       asset.DeviceID,
		FileCreatedAt:  asset.CapturedAt.Format(time.RFC3339Nano),
		FileModifiedAt: asset.FileModifiedAt.Format(time.RFC3339Nano),
		IsFavorite:     strconv.FormatBool(asset.IsFavorite),
		Filename:       asset.Filename,
		ContentType:    asset.MediaType,
		ICloudId:       asset.ICloudID,
		Latitude:       asset.Latitude,
		Longitude:      asset.Longitude,
		SourceChecksum: asset.SourceChecksum,
	}
	body, contentType, contentLength, err := multipartFileBody(metadata, file, info.Size())
	if err != nil {
		file.Close()
		return "", false, 0, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.immichURL.String()+"/api/assets", body)
	if err != nil {
		body.Close()
		return "", false, 0, nil, err
	}
	req.ContentLength = contentLength
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Accept", "application/json")
	if s.cfg.ImmichAPIKey != "" {
		req.Header.Set("x-api-key", s.cfg.ImmichAPIKey)
	} else {
		copyClientAuth(req.Header, clientHeaders)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", false, 0, nil, err
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return "", false, 0, nil, err
	}
	if len(responseBody) > maxResponseBytes {
		return "", false, 0, nil, errors.New("Immich response exceeded size limit")
	}
	var response struct {
		ID        string `json:"id"`
		Duplicate bool   `json:"duplicate"`
	}
	_ = json.Unmarshal(responseBody, &response)
	return response.ID, response.Duplicate, resp.StatusCode, responseBody, nil
}

func multipartFileBody(metadata handlers.BackgroundUploadRequest, file *os.File, fileSize int64) (io.ReadCloser, string, int64, error) {
	var buffer bytes.Buffer
	writer := multipart.NewWriter(&buffer)
	fields := [][2]string{
		{"deviceAssetId", metadata.DeviceAssetID},
		{"deviceId", metadata.DeviceID},
		{"fileCreatedAt", metadata.FileCreatedAt},
		{"fileModifiedAt", metadata.FileModifiedAt},
		{"isFavorite", metadata.IsFavorite},
	}
	for _, field := range fields {
		if err := writer.WriteField(field[0], field[1]); err != nil {
			return nil, "", 0, err
		}
	}
	if metadata.ICloudId != "" || metadata.SourceChecksum != "" || metadata.Latitude != "" || metadata.Longitude != "" {
		value := handlers.MobileAppMetadata{
			ICloudId:       metadata.ICloudId,
			CreatedAt:      metadata.FileCreatedAt,
			Latitude:       metadata.Latitude,
			Longitude:      metadata.Longitude,
			SourceChecksum: metadata.SourceChecksum,
		}
		encoded, err := json.Marshal([]handlers.RemoteAssetMetadataItem{{Key: "mobile-app", Value: value}})
		if err != nil {
			return nil, "", 0, err
		}
		if err := writer.WriteField("metadata", string(encoded)); err != nil {
			return nil, "", 0, err
		}
	}
	mediaType := firstNonEmpty(metadata.ContentType, "application/octet-stream")
	partHeader := make(textproto.MIMEHeader)
	partHeader.Set("Content-Disposition", mime.FormatMediaType("form-data", map[string]string{
		"name":     "assetData",
		"filename": metadata.Filename,
	}))
	partHeader.Set("Content-Type", mediaType)
	if _, err := writer.CreatePart(partHeader); err != nil {
		return nil, "", 0, err
	}
	prefixLength := buffer.Len()
	if err := writer.Close(); err != nil {
		return nil, "", 0, err
	}
	prefix := bytes.Clone(buffer.Bytes()[:prefixLength])
	suffix := bytes.Clone(buffer.Bytes()[prefixLength:])
	stream := io.MultiReader(bytes.NewReader(prefix), file, bytes.NewReader(suffix))
	contentLength := int64(len(prefix)) + fileSize + int64(len(suffix))
	return struct {
		io.Reader
		io.Closer
	}{Reader: stream, Closer: file}, writer.FormDataContentType(), contentLength, nil
}

func (s *photoBackupServer) handleArchiveAPI(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Token == "" {
		writeAPIError(w, http.StatusNotFound, "archive API is disabled")
		return
	}
	if !s.validStandaloneBearer(r) {
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeAPIError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/archive/assets")
	if path == "" || path == "/" {
		assets, err := s.store.ListAssets(queryInt(r, "limit", 200))
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "could not list archived assets")
			return
		}
		writeJSON(w, http.StatusOK, assets)
		return
	}
	id := strings.TrimPrefix(path, "/")
	if strings.Contains(id, "/") {
		writeAPIError(w, http.StatusNotFound, "asset not found")
		return
	}
	asset, filePath, err := s.store.GetAsset(id)
	if err != nil {
		writeAPIError(w, http.StatusNotFound, "asset not found")
		return
	}
	file, err := os.Open(filePath)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "could not open archived asset")
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "could not read archived asset")
		return
	}
	w.Header().Set("Content-Type", firstNonEmpty(asset.MediaType, "application/octet-stream"))
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": asset.Filename}))
	http.ServeContent(w, r, asset.Filename, info.ModTime(), file)
}

func (s *photoBackupServer) validStandaloneBearer(r *http.Request) bool {
	if s.cfg.Token == "" {
		return false
	}
	value := strings.TrimSpace(r.Header.Get("Authorization"))
	prefix := "Bearer "
	if len(value) <= len(prefix) || !strings.EqualFold(value[:len(prefix)], prefix) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(strings.TrimSpace(value[len(prefix):])), []byte(s.cfg.Token)) == 1
}

func (s *photoBackupServer) startForwardWorker() {
	if s.immich == nil || s.cfg.ImmichAPIKey == "" {
		return
	}
	go func() {
		s.forwardPendingAssets()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			s.forwardPendingAssets()
		}
	}()
}

func (s *photoBackupServer) forwardPendingAssets() {
	assets, err := s.store.PendingAssets(100)
	if err != nil {
		log.Printf("Could not load pending archive uploads: %v", err)
		return
	}
	for _, pending := range assets {
		if !s.claimForward(pending.Asset.ID) {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
		remoteID, _, status, _, forwardErr := s.forwardAsset(ctx, nil, pending.Asset, pending.FilePath)
		cancel()
		if forwardErr != nil {
			_ = s.store.MarkForwardResult(pending.Asset.ID, "", "Immich request failed")
			log.Printf("Forwarding archived asset %s failed: %v", pending.Asset.ID, forwardErr)
			s.releaseForward(pending.Asset.ID)
			continue
		}
		if status < http.StatusOK || status >= http.StatusMultipleChoices {
			_ = s.store.MarkForwardResult(pending.Asset.ID, "", fmt.Sprintf("Immich returned HTTP %d", status))
			log.Printf("Forwarding archived asset %s failed with HTTP %d", pending.Asset.ID, status)
			s.releaseForward(pending.Asset.ID)
			continue
		}
		if remoteID == "" {
			_ = s.store.MarkForwardResult(pending.Asset.ID, "", "Immich response did not include an asset id")
			log.Printf("Forwarding archived asset %s returned no Immich id", pending.Asset.ID)
			s.releaseForward(pending.Asset.ID)
			continue
		}
		if err := s.store.MarkForwardResult(pending.Asset.ID, remoteID, ""); err != nil {
			log.Printf("Could not save forwarding status for archive asset %s: %v", pending.Asset.ID, err)
		}
		s.releaseForward(pending.Asset.ID)
	}
}

func (s *photoBackupServer) claimForward(id string) bool {
	s.forwardMu.Lock()
	defer s.forwardMu.Unlock()
	if _, exists := s.forwarding[id]; exists {
		return false
	}
	s.forwarding[id] = struct{}{}
	return true
}

func (s *photoBackupServer) releaseForward(id string) {
	s.forwardMu.Lock()
	delete(s.forwarding, id)
	s.forwardMu.Unlock()
}

func hasClientAuth(r *http.Request) bool {
	return clientCredential(r) != ""
}

func clientCredential(r *http.Request) string {
	authorization := strings.TrimSpace(r.Header.Get("Authorization"))
	prefix := "Bearer "
	if len(authorization) > len(prefix) && strings.EqualFold(authorization[:len(prefix)], prefix) {
		return strings.TrimSpace(authorization[len(prefix):])
	}
	return strings.TrimSpace(r.Header.Get("x-api-key"))
}

func copyClientAuth(destination, source http.Header) {
	for _, key := range []string{"Authorization", "x-api-key"} {
		if value := source.Get(key); value != "" {
			destination.Set(key, value)
		}
	}
}

func parseMobileMetadata(raw string) (cloudID, latitude, longitude, checksum, createdAt string) {
	if len(raw) == 0 || len(raw) > maxMetadataBytes {
		return
	}
	var values []struct {
		Key   string `json:"key"`
		Value struct {
			ICloudID       string `json:"iCloudId"`
			Latitude       string `json:"latitude"`
			Longitude      string `json:"longitude"`
			SourceChecksum string `json:"sourceChecksum"`
			CreatedAt      string `json:"createdAt"`
		} `json:"value"`
	}
	if json.Unmarshal([]byte(raw), &values) != nil {
		return
	}
	for _, item := range values {
		if item.Key == "mobile-app" {
			return item.Value.ICloudID, item.Value.Latitude, item.Value.Longitude, item.Value.SourceChecksum, item.Value.CreatedAt
		}
	}
	return
}

func parseTimestamp(value string) time.Time {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.999999999Z07:00"} {
		if parsed, err := time.Parse(layout, strings.TrimSpace(value)); err == nil {
			return parsed.UTC()
		}
	}
	return time.Now().UTC()
}

func firstNonZeroTime(value, fallback time.Time) time.Time {
	if value.IsZero() {
		return fallback
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func parseBool(value string) bool {
	parsed, _ := strconv.ParseBool(strings.TrimSpace(value))
	return parsed
}

func resourceType(mediaType string) string {
	if strings.HasPrefix(strings.ToLower(mediaType), "video/") {
		return "VIDEO"
	}
	return "IMAGE"
}

func queryInt(r *http.Request, key string, fallback int) int {
	value, err := strconv.Atoi(r.URL.Query().Get(key))
	if err != nil || value < 1 || value > 1000 {
		return fallback
	}
	return value
}

func writeUploadError(w http.ResponseWriter, err error) {
	var maxBytesError *http.MaxBytesError
	if errors.As(err, &maxBytesError) || strings.Contains(strings.ToLower(err.Error()), "exceeds configured size limit") {
		writeAPIError(w, http.StatusRequestEntityTooLarge, "upload exceeds configured size limit")
		return
	}
	writeAPIError(w, http.StatusInternalServerError, "upload could not be stored")
}

func multipartRequestLimit(fileLimit int64) int64 {
	const maxInt64 = int64(^uint64(0) >> 1)
	if fileLimit > maxInt64-maxMetadataBytes {
		return maxInt64
	}
	return fileLimit + maxMetadataBytes
}

func writeAPIError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"message": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
