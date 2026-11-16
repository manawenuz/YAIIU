package photobackup

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	uploadLimitHeader      = "Upload-Limit"
	uploadOffsetHeader     = "Upload-Offset"
	uploadCompleteHeader   = "Upload-Complete"
	legacyIncompleteHeader = "Upload-Incomplete"
	interopVersionHeader   = "Upload-Draft-Interop-Version"
	currentInteropVersion  = "10"
	progressChunkBytes     = 8 << 20
)

type Handler struct {
	cfg   Config
	store *Store
}

func NewHandler(cfg Config, store *Store) *Handler {
	return &Handler{cfg: cfg, store: store}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}

	if !h.authorized(r) {
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	if r.URL.Path == "/upload" {
		h.handleUploadRoot(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/upload/") {
		h.handleUploadResource(w, r)
		return
	}
	if r.URL.Path == "/api/assets" {
		h.handleAssetList(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/assets/") {
		h.handleAsset(w, r)
		return
	}
	writeError(w, http.StatusNotFound, "not found")
}

func (h *Handler) handleUploadRoot(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodOptions:
		h.handleOptions(w, r)
	case http.MethodPost:
		h.handleCreateUpload(w, r)
	default:
		w.Header().Set("Allow", "OPTIONS, POST")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *Handler) handleOptions(w http.ResponseWriter, r *http.Request) {
	if err := checkInteropVersion(r); err != nil {
		writeError(w, http.StatusNotImplemented, err.Error())
		return
	}
	modern := isModernProtocol(r)
	w.Header().Set(uploadLimitHeader, h.uploadLimitValue(modern))
	w.Header().Set("Allow", "OPTIONS, POST")
	w.Header().Set("Accept-Patch", "application/partial-upload")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleCreateUpload(w http.ResponseWriter, r *http.Request) {
	if err := checkInteropVersion(r); err != nil {
		writeError(w, http.StatusNotImplemented, err.Error())
		return
	}
	modern := isModernProtocol(r)
	complete, present, err := uploadCompleteFromHeaders(r.Header)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !present {
		// A plain POST is also useful for command-line imports. PhotoKit clients
		// send one of the resumable-upload completeness headers.
		complete = true
	}

	expectedLength := int64(-1)
	if raw := strings.TrimSpace(r.Header.Get("Upload-Length")); raw != "" {
		expectedLength, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || expectedLength < 0 {
			writeError(w, http.StatusBadRequest, "invalid Upload-Length")
			return
		}
		if expectedLength > h.cfg.MaxUploadBytes {
			writeError(w, http.StatusRequestEntityTooLarge, "upload exceeds configured size limit")
			return
		}
	}
	if r.ContentLength > h.cfg.MaxUploadBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "upload exceeds configured size limit")
		return
	}

	filename := safeFilename(r.Header.Get("X-Photo-Backup-Filename"))
	if filename == "" {
		filename = safeFilename(r.Header.Get("X-Filename"))
	}
	if filename == "" {
		filename = "upload.bin"
	}
	capturedAt := time.Now().UTC()
	if raw := strings.TrimSpace(r.Header.Get("X-Photo-Backup-Captured-At")); raw != "" {
		parsed, parseErr := time.Parse(time.RFC3339Nano, raw)
		if parseErr != nil {
			writeError(w, http.StatusBadRequest, "X-Photo-Backup-Captured-At must be RFC3339")
			return
		}
		capturedAt = parsed.UTC()
	}

	session, err := h.store.CreateSession(
		safeHeader(r.Header.Get("X-Photo-Backup-Asset-ID")),
		safeHeader(r.Header.Get("X-Photo-Backup-Resource-Type")),
		filename,
		safeHeader(r.Header.Get("Content-Type")),
		capturedAt,
		expectedLength,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not create upload")
		return
	}

	session.mu.Lock()
	defer session.mu.Unlock()
	location := h.uploadLocation(r, session.state.ID)
	h.sendResumptionNotice(w, r, location, true, 0, modern)
	if _, err := h.receiveBody(w, r, session, 0, location, modern); err != nil {
		if errors.Is(err, errUploadTooLarge) {
			writeProtocolError(w, http.StatusRequestEntityTooLarge, "upload exceeds configured size limit", session.state.Offset, false, modern)
			return
		}
		if errors.Is(err, errLengthExceeded) {
			writeProtocolError(w, http.StatusBadRequest, "upload exceeds declared Upload-Length", session.state.Offset, false, modern)
			return
		}
		if errors.Is(err, errLengthMismatch) {
			writeProtocolError(w, http.StatusBadRequest, "upload length does not match Upload-Length", session.state.Offset, false, modern)
			return
		}
		writeProtocolError(w, http.StatusInternalServerError, "upload interrupted; resume at the returned offset", session.state.Offset, false, modern)
		return
	}
	if complete {
		if session.state.ExpectedLength >= 0 && session.state.Offset != session.state.ExpectedLength {
			writeProtocolError(w, http.StatusBadRequest, "completed upload does not match Upload-Length", session.state.Offset, false, modern)
			return
		}
		asset, err := h.store.Finalize(session)
		if err != nil {
			writeProtocolError(w, http.StatusInternalServerError, "upload received but could not be committed", session.state.Offset, false, modern)
			return
		}
		w.Header().Set("Location", location)
		h.setCompleteHeaders(w, true, modern)
		w.Header().Set(uploadOffsetHeader, strconv.FormatInt(asset.Size, 10))
		w.Header().Set(uploadLimitHeader, h.uploadLimitValue(modern))
		w.Header().Set("X-Server-Resource-ID", asset.ID)
		writeJSON(w, http.StatusCreated, asset)
		return
	}

	h.setCompleteHeaders(w, false, modern)
	w.Header().Set("Location", location)
	w.Header().Set(uploadOffsetHeader, strconv.FormatInt(session.state.Offset, 10))
	w.Header().Set(uploadLimitHeader, h.uploadLimitValue(modern))
	w.WriteHeader(http.StatusCreated)
}

func (h *Handler) handleUploadResource(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/upload/")
	if id == "" || strings.Contains(id, "/") || !validID(id) {
		writeError(w, http.StatusNotFound, "upload not found")
		return
	}
	if err := checkInteropVersion(r); err != nil {
		writeError(w, http.StatusNotImplemented, err.Error())
		return
	}
	modern := isModernProtocol(r)
	session, err := h.store.GetSession(id)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			writeError(w, http.StatusNotFound, "upload not found")
		} else {
			writeError(w, http.StatusInternalServerError, "could not read upload state")
		}
		return
	}

	session.mu.Lock()
	defer session.mu.Unlock()
	if session.deleted {
		writeError(w, http.StatusNotFound, "upload not found")
		return
	}
	switch r.Method {
	case http.MethodHead, http.MethodGet:
		h.writeOffsetResponse(w, session, modern)
	case http.MethodPatch:
		h.handleAppend(w, r, session, modern)
	case http.MethodDelete:
		if session.state.Complete {
			writeProtocolError(w, http.StatusConflict, "completed upload cannot be cancelled", session.state.Offset, true, modern)
			return
		}
		if err := os.RemoveAll(filepath.Dir(h.store.PartPath(id))); err != nil {
			writeError(w, http.StatusInternalServerError, "could not cancel upload")
			return
		}
		session.deleted = true
		h.store.ForgetSession(id)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "GET, HEAD, PATCH, DELETE")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *Handler) handleAppend(w http.ResponseWriter, r *http.Request, session *uploadSession, modern bool) {
	if session.state.Complete {
		writeProtocolError(w, http.StatusConflict, "upload is already complete", session.state.Offset, true, modern)
		return
	}
	complete, present, err := uploadCompleteFromHeaders(r.Header)
	if err != nil || !present {
		if err == nil {
			err = fmt.Errorf("missing Upload-Complete")
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	offset, err := strconv.ParseInt(strings.TrimSpace(r.Header.Get(uploadOffsetHeader)), 10, 64)
	if err != nil || offset < 0 {
		writeError(w, http.StatusBadRequest, "missing or invalid Upload-Offset")
		return
	}
	if offset != session.state.Offset {
		writeProtocolError(w, http.StatusConflict, "Upload-Offset does not match server offset", session.state.Offset, false, modern)
		return
	}
	if encoding := r.Header.Get("Content-Encoding"); encoding != "" && !strings.EqualFold(encoding, "identity") {
		writeProtocolError(w, http.StatusUnsupportedMediaType, "Content-Encoding is not supported for resumable uploads", session.state.Offset, false, modern)
		return
	}
	if mediaType := r.Header.Get("Content-Type"); mediaType != "" &&
		!strings.EqualFold(strings.TrimSpace(strings.Split(mediaType, ";")[0]), "application/partial-upload") {
		writeProtocolError(w, http.StatusUnsupportedMediaType, "PATCH Content-Type must be application/partial-upload", session.state.Offset, false, modern)
		return
	}
	if r.ContentLength > h.cfg.MaxUploadBytes-session.state.Offset {
		writeProtocolError(w, http.StatusRequestEntityTooLarge, "upload exceeds configured size limit", session.state.Offset, false, modern)
		return
	}

	location := h.uploadLocation(r, session.state.ID)
	h.sendResumptionNotice(w, r, location, false, session.state.Offset, modern)
	if _, err := h.receiveBody(w, r, session, session.state.Offset, location, modern); err != nil {
		switch {
		case errors.Is(err, errUploadTooLarge):
			writeProtocolError(w, http.StatusRequestEntityTooLarge, "upload exceeds configured size limit", session.state.Offset, false, modern)
		case errors.Is(err, errLengthExceeded):
			writeProtocolError(w, http.StatusBadRequest, "upload exceeds declared Upload-Length", session.state.Offset, false, modern)
		default:
			writeProtocolError(w, http.StatusInternalServerError, "upload interrupted; resume at the returned offset", session.state.Offset, false, modern)
		}
		return
	}

	if complete {
		if session.state.ExpectedLength >= 0 && session.state.Offset != session.state.ExpectedLength {
			writeProtocolError(w, http.StatusBadRequest, "completed upload does not match Upload-Length", session.state.Offset, false, modern)
			return
		}
		asset, err := h.store.Finalize(session)
		if err != nil {
			writeProtocolError(w, http.StatusInternalServerError, "upload received but could not be committed", session.state.Offset, false, modern)
			return
		}
		w.Header().Set("Location", location)
		h.setCompleteHeaders(w, true, modern)
		w.Header().Set(uploadOffsetHeader, strconv.FormatInt(asset.Size, 10))
		w.Header().Set("X-Server-Resource-ID", asset.ID)
		writeJSON(w, http.StatusOK, asset)
		return
	}

	h.setCompleteHeaders(w, false, modern)
	w.Header().Set(uploadOffsetHeader, strconv.FormatInt(session.state.Offset, 10))
	w.Header().Set(uploadLimitHeader, h.uploadLimitValue(modern))
	w.WriteHeader(http.StatusNoContent)
}

var (
	errUploadTooLarge = errors.New("upload too large")
	errLengthExceeded = errors.New("declared length exceeded")
	errLengthMismatch = errors.New("declared length mismatch")
)

func (h *Handler) receiveBody(w http.ResponseWriter, r *http.Request, session *uploadSession, baseOffset int64, location string, modern bool) (int64, error) {
	if session.state.Complete {
		return 0, fmt.Errorf("upload is already complete")
	}
	part, err := os.OpenFile(h.store.PartPath(session.state.ID), os.O_WRONLY, 0o600)
	if err != nil {
		return 0, err
	}
	if baseOffset == 0 {
		if err := part.Truncate(0); err != nil {
			_ = part.Close()
			return 0, err
		}
	} else if _, err := part.Seek(0, io.SeekEnd); err != nil {
		_ = part.Close()
		return 0, err
	}
	defer part.Close()

	buf := make([]byte, 256<<10)
	var received int64
	var lastProgress int64
	for {
		n, readErr := r.Body.Read(buf)
		if n > 0 {
			if received+int64(n) > h.cfg.MaxUploadBytes-baseOffset {
				return received, errUploadTooLarge
			}
			if session.state.ExpectedLength >= 0 && baseOffset+received+int64(n) > session.state.ExpectedLength {
				return received, errLengthExceeded
			}
			written, writeErr := part.Write(buf[:n])
			received += int64(written)
			if writeErr != nil {
				return received, writeErr
			}
			if written != n {
				return received, io.ErrShortWrite
			}
			if received-lastProgress >= progressChunkBytes {
				if err := h.commitProgress(part, session, baseOffset+received); err != nil {
					return received, err
				}
				lastProgress = received
				h.sendResumptionNotice(w, r, location, false, session.state.Offset, modern)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			if received > 0 {
				_ = h.commitProgress(part, session, baseOffset+received)
			}
			return received, readErr
		}
	}
	if err := h.commitProgress(part, session, baseOffset+received); err != nil {
		return received, err
	}
	return received, nil
}

func (h *Handler) commitProgress(part *os.File, session *uploadSession, offset int64) error {
	if err := part.Sync(); err != nil {
		return err
	}
	previousOffset := session.state.Offset
	session.state.Offset = offset
	if err := h.store.Persist(session); err != nil {
		session.state.Offset = previousOffset
		return err
	}
	return nil
}

func (h *Handler) writeOffsetResponse(w http.ResponseWriter, session *uploadSession, modern bool) {
	w.Header().Set(uploadOffsetHeader, strconv.FormatInt(session.state.Offset, 10))
	w.Header().Set(uploadLimitHeader, h.uploadLimitValue(modern))
	w.Header().Set("Cache-Control", "no-store")
	h.setCompleteHeaders(w, session.state.Complete, modern)
	if session.state.Complete {
		w.Header().Set("X-Server-Resource-ID", session.state.ID)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) sendResumptionNotice(w http.ResponseWriter, r *http.Request, location string, creating bool, offset int64, modern bool) {
	w.Header().Set(uploadLimitHeader, h.uploadLimitValue(modern))
	if creating {
		w.Header().Set("Location", location)
	} else {
		w.Header().Set(uploadOffsetHeader, strconv.FormatInt(offset, 10))
	}
	if version := strings.TrimSpace(r.Header.Get(interopVersionHeader)); version != "" {
		w.Header().Set(interopVersionHeader, version)
	}
	w.WriteHeader(104)
	_ = http.NewResponseController(w).Flush()
	w.Header().Del(uploadLimitHeader)
	w.Header().Del("Location")
	w.Header().Del(uploadOffsetHeader)
	w.Header().Del(interopVersionHeader)
}

func (h *Handler) sendAssetList(w http.ResponseWriter, r *http.Request) {
	limit := 200
	if raw := r.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 1000 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 1000")
			return
		}
		limit = value
	}
	assets, err := h.store.ListAssets(limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not list stored assets")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"count": len(assets), "assets": assets})
}

func (h *Handler) handleAssetList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	h.sendAssetList(w, r)
}

func (h *Handler) handleAsset(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/assets/")
	parts := strings.Split(rest, "/")
	if len(parts) < 1 || len(parts) > 2 || parts[0] == "" {
		writeError(w, http.StatusNotFound, "asset not found")
		return
	}
	asset, path, err := h.store.GetAsset(parts[0])
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			writeError(w, http.StatusNotFound, "asset not found")
		} else {
			writeError(w, http.StatusInternalServerError, "could not read asset")
		}
		return
	}
	if len(parts) == 1 {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		writeJSON(w, http.StatusOK, asset)
		return
	}
	if parts[1] != "content" || (r.Method != http.MethodGet && r.Method != http.MethodHead) {
		w.Header().Set("Allow", "GET, HEAD")
		writeError(w, http.StatusNotFound, "asset content not found")
		return
	}
	file, err := os.Open(path)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not open asset content")
		return
	}
	defer file.Close()
	if asset.MediaType != "" {
		w.Header().Set("Content-Type", asset.MediaType)
	}
	if disposition := mime.FormatMediaType("attachment", map[string]string{"filename": asset.Filename}); disposition != "" {
		w.Header().Set("Content-Disposition", disposition)
	}
	http.ServeContent(w, r, asset.Filename, asset.UploadedAt, file)
}

func (h *Handler) uploadLocation(r *http.Request, id string) string {
	base := h.cfg.PublicBaseURL
	if base == "" {
		scheme := "http"
		if r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") {
			scheme = "https"
		}
		base = scheme + "://" + r.Host
	}
	return strings.TrimRight(base, "/") + "/upload/" + id
}

func (h *Handler) uploadLimitValue(modern bool) string {
	value := strconv.FormatInt(h.cfg.MaxUploadBytes, 10)
	if modern {
		return "max-size=" + value
	}
	return value
}

func (h *Handler) setCompleteHeaders(w http.ResponseWriter, complete, modern bool) {
	setUploadCompleteHeaders(w.Header(), complete, modern)
}

func (h *Handler) authorized(r *http.Request) bool {
	fields := strings.Fields(r.Header.Get("Authorization"))
	if len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(fields[1]), []byte(h.cfg.Token)) == 1
}

func checkInteropVersion(r *http.Request) error {
	version := strings.TrimSpace(r.Header.Get(interopVersionHeader))
	if version != "" && version != currentInteropVersion {
		return fmt.Errorf("unsupported Upload-Draft-Interop-Version %q; supported version is %s", version, currentInteropVersion)
	}
	return nil
}

func isModernProtocol(r *http.Request) bool {
	return strings.TrimSpace(r.Header.Get(interopVersionHeader)) != "" || r.Header.Get(uploadCompleteHeader) != ""
}

func uploadCompleteFromHeaders(header http.Header) (bool, bool, error) {
	if value := strings.TrimSpace(header.Get(uploadCompleteHeader)); value != "" {
		complete, err := parseStructuredBool(value)
		return complete, true, err
	}
	if value := strings.TrimSpace(header.Get(legacyIncompleteHeader)); value != "" {
		incomplete, err := parseStructuredBool(value)
		return !incomplete, true, err
	}
	return false, false, nil
}

func parseStructuredBool(value string) (bool, error) {
	switch value {
	case "?1":
		return true, nil
	case "?0":
		return false, nil
	default:
		return false, fmt.Errorf("expected a structured boolean (?1 or ?0)")
	}
}

func safeFilename(value string) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, "\\", "/"))
	value = filepath.Base(value)
	value = strings.Map(func(r rune) rune {
		if r < 32 || r == 127 || r == ':' {
			return -1
		}
		return r
	}, value)
	value = strings.Trim(value, " .")
	if len(value) > 180 {
		ext := filepath.Ext(value)
		base := strings.TrimSuffix(value, ext)
		for len(base) > 180-len(ext) {
			_, size := utf8DecodeLastRune(base)
			base = base[:len(base)-size]
		}
		value = base + ext
	}
	return value
}

func safeHeader(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 512 {
		return value[:512]
	}
	return value
}

func utf8DecodeLastRune(value string) (rune, int) {
	for i := len(value) - 1; i >= 0; i-- {
		if value[i]&0xc0 != 0x80 {
			return rune(value[i]), len(value) - i
		}
	}
	return 0, len(value)
}

func writeProtocolError(w http.ResponseWriter, status int, message string, offset int64, complete, modern bool) {
	w.Header().Set(uploadOffsetHeader, strconv.FormatInt(offset, 10))
	setUploadCompleteHeaders(w.Header(), complete, modern)
	writeError(w, status, message)
}

func setUploadCompleteHeaders(header http.Header, complete, modern bool) {
	if modern {
		value := "?0"
		if complete {
			value = "?1"
		}
		header.Set(uploadCompleteHeader, value)
		header.Del(legacyIncompleteHeader)
	} else {
		header.Del(uploadCompleteHeader)
		incomplete := "?1"
		if complete {
			incomplete = "?0"
		}
		header.Set(legacyIncompleteHeader, incomplete)
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
