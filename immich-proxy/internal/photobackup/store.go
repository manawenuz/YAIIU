package photobackup

import (
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type Asset struct {
	ID              string    `json:"id"`
	DeviceID        string    `json:"deviceId,omitempty"`
	DeviceAssetID   string    `json:"deviceAssetId,omitempty"`
	AssetVersion    string    `json:"assetVersion,omitempty"`
	ResourceType    string    `json:"resourceType,omitempty"`
	Filename        string    `json:"filename"`
	MediaType       string    `json:"mediaType"`
	CapturedAt      time.Time `json:"capturedAt"`
	FileModifiedAt  time.Time `json:"fileModifiedAt,omitempty"`
	IsFavorite      bool      `json:"isFavorite,omitempty"`
	ICloudID        string    `json:"iCloudId,omitempty"`
	Latitude        string    `json:"latitude,omitempty"`
	Longitude       string    `json:"longitude,omitempty"`
	TimezoneOffset  string    `json:"timezoneOffset,omitempty"`
	SourceChecksum  string    `json:"sourceChecksum,omitempty"`
	UploadedAt      time.Time `json:"uploadedAt"`
	Size            int64     `json:"size"`
	SHA256          string    `json:"sha256"`
	SHA1            string    `json:"sha1"`
	ImmichAssetID   string    `json:"immichAssetId,omitempty"`
	ForwardedAt     time.Time `json:"forwardedAt,omitempty"`
	ForwardError    string    `json:"forwardError,omitempty"`
	ForwardAttempts int       `json:"forwardAttempts,omitempty"`
	ForwardRetryAt  time.Time `json:"forwardRetryAt,omitempty"`
}

type PendingAsset struct {
	Asset    Asset
	FilePath string
}

type assetRecord struct {
	Asset
	File           string `json:"file"`
	IdempotencyKey string `json:"idempotencyKey,omitempty"`
}

type sessionState struct {
	ID             string    `json:"id"`
	DeviceID       string    `json:"deviceId,omitempty"`
	DeviceAssetID  string    `json:"deviceAssetId,omitempty"`
	AssetVersion   string    `json:"assetVersion,omitempty"`
	ResourceType   string    `json:"resourceType,omitempty"`
	Filename       string    `json:"filename"`
	MediaType      string    `json:"mediaType"`
	CapturedAt     time.Time `json:"capturedAt"`
	FileModifiedAt time.Time `json:"fileModifiedAt,omitempty"`
	IsFavorite     bool      `json:"isFavorite,omitempty"`
	ICloudID       string    `json:"iCloudId,omitempty"`
	Latitude       string    `json:"latitude,omitempty"`
	Longitude      string    `json:"longitude,omitempty"`
	TimezoneOffset string    `json:"timezoneOffset,omitempty"`
	SourceChecksum string    `json:"sourceChecksum,omitempty"`
	CreatedAt      time.Time `json:"createdAt"`
	ExpectedLength int64     `json:"expectedLength"`
	Offset         int64     `json:"offset"`
	Complete       bool      `json:"complete"`
	Duplicate      bool      `json:"duplicate,omitempty"`
	StoredAssetID  string    `json:"storedAssetId,omitempty"`
	AssetDirectory string    `json:"assetDirectory,omitempty"`
}

type UploadMetadata struct {
	DeviceID       string
	DeviceAssetID  string
	AssetVersion   string
	ResourceType   string
	Filename       string
	MediaType      string
	CapturedAt     time.Time
	FileModifiedAt time.Time
	IsFavorite     bool
	ICloudID       string
	Latitude       string
	Longitude      string
	TimezoneOffset string
	SourceChecksum string
}

type uploadSession struct {
	mu      sync.Mutex
	state   sessionState
	deleted bool
}

type Store struct {
	root           string
	maxUploadBytes int64
	mu             sync.Mutex
	sessions       map[string]*uploadSession
	assetMu        sync.Mutex
	assetIndex     map[string]assetRecord
}

func NewStore(root string, maxUploadBytes int64) (*Store, error) {
	if maxUploadBytes < 1 {
		return nil, fmt.Errorf("maximum upload size must be positive")
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve data directory: %w", err)
	}
	for _, dir := range []string{
		absRoot,
		filepath.Join(absRoot, ".staging"),
		filepath.Join(absRoot, "originals"),
	} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("create data directory %s: %w", dir, err)
		}
	}
	store := &Store{
		root:           absRoot,
		maxUploadBytes: maxUploadBytes,
		sessions:       make(map[string]*uploadSession),
		assetIndex:     make(map[string]assetRecord),
	}
	if err := store.loadAssetIndex(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *Store) CreateSession(deviceAssetID, resourceType, filename, mediaType string, capturedAt time.Time, expectedLength int64) (*uploadSession, error) {
	if expectedLength < -1 || expectedLength > s.maxUploadBytes {
		return nil, fmt.Errorf("expected upload length is outside the configured limit")
	}
	id, err := newID()
	if err != nil {
		return nil, fmt.Errorf("generate upload id: %w", err)
	}
	now := time.Now().UTC()
	if filename == "" {
		filename = "upload.bin"
	}
	if mediaType == "" {
		mediaType = mime.TypeByExtension(filepath.Ext(filename))
		if mediaType == "" {
			mediaType = "application/octet-stream"
		}
	}
	state := sessionState{
		ID:             id,
		DeviceAssetID:  deviceAssetID,
		ResourceType:   resourceType,
		Filename:       filename,
		MediaType:      mediaType,
		CapturedAt:     capturedAt.UTC(),
		CreatedAt:      now,
		ExpectedLength: expectedLength,
		Offset:         0,
	}
	handle := &uploadSession{state: state}
	dir := s.sessionDir(id)
	if err := os.Mkdir(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create upload session: %w", err)
	}
	part, err := os.OpenFile(s.partPath(id), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("create upload file: %w", err)
	}
	if err := part.Close(); err != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("close upload file: %w", err)
	}
	if err := s.persist(handle); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	s.mu.Lock()
	s.sessions[id] = handle
	s.mu.Unlock()
	return handle, nil
}

func (s *Store) SaveUpload(metadata UploadMetadata, source io.Reader) (Asset, bool, error) {
	if source == nil {
		return Asset{}, false, fmt.Errorf("upload body is required")
	}
	metadata.Filename = safeFilename(metadata.Filename)
	if metadata.Filename == "" {
		metadata.Filename = "upload.bin"
	}
	if metadata.CapturedAt.IsZero() {
		metadata.CapturedAt = time.Now().UTC()
	}
	if metadata.FileModifiedAt.IsZero() {
		metadata.FileModifiedAt = metadata.CapturedAt
	}
	if metadata.DeviceID == "" {
		metadata.DeviceID = "unknown-device"
	}

	handle, err := s.CreateSession(metadata.DeviceAssetID, metadata.ResourceType, metadata.Filename, metadata.MediaType, metadata.CapturedAt, -1)
	if err != nil {
		return Asset{}, false, err
	}
	handle.mu.Lock()
	defer handle.mu.Unlock()
	handle.state.DeviceID = metadata.DeviceID
	handle.state.AssetVersion = metadata.AssetVersion
	handle.state.FileModifiedAt = metadata.FileModifiedAt.UTC()
	handle.state.IsFavorite = metadata.IsFavorite
	handle.state.ICloudID = metadata.ICloudID
	handle.state.Latitude = metadata.Latitude
	handle.state.Longitude = metadata.Longitude
	handle.state.TimezoneOffset = metadata.TimezoneOffset
	handle.state.SourceChecksum = metadata.SourceChecksum
	if err := s.persist(handle); err != nil {
		s.discardUpload(handle.state.ID)
		return Asset{}, false, err
	}

	part, err := os.OpenFile(s.partPath(handle.state.ID), os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		s.discardUpload(handle.state.ID)
		return Asset{}, false, fmt.Errorf("open staged upload: %w", err)
	}
	limited := &io.LimitedReader{R: source, N: s.maxUploadBytes + 1}
	size, copyErr := io.Copy(part, limited)
	syncErr := part.Sync()
	closeErr := part.Close()
	if copyErr != nil || syncErr != nil || closeErr != nil {
		s.discardUpload(handle.state.ID)
		return Asset{}, false, errors.Join(copyErr, syncErr, closeErr)
	}
	if size > s.maxUploadBytes {
		s.discardUpload(handle.state.ID)
		return Asset{}, false, fmt.Errorf("upload exceeds configured size limit")
	}
	handle.state.ExpectedLength = size
	handle.state.Offset = size
	if err := s.persist(handle); err != nil {
		s.discardUpload(handle.state.ID)
		return Asset{}, false, err
	}
	return s.FinalizeWithDuplicate(handle)
}

func (s *Store) discardUpload(id string) {
	_ = os.RemoveAll(s.sessionDir(id))
	s.ForgetSession(id)
}

func (s *Store) loadAssetIndex() error {
	root := filepath.Join(s.root, "originals")
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() != "asset.json" {
			return nil
		}
		record, err := readAssetRecord(path)
		if err != nil {
			return fmt.Errorf("load asset index from %s: %w", path, err)
		}
		if err := s.repairLegacyAssetFilePath(path, &record); err != nil {
			return fmt.Errorf("repair asset file path from %s: %w", path, err)
		}
		key := record.IdempotencyKey
		if key == "" {
			key = makeIdempotencyKey(record.DeviceID, record.DeviceAssetID, record.AssetVersion)
		}
		if key != "" {
			current, exists := s.assetIndex[key]
			if !exists || record.UploadedAt.After(current.UploadedAt) {
				s.assetIndex[key] = record
			}
		}
		return nil
	})
}

// repairLegacyAssetFilePath fixes records written by versions that stored a
// temporary staging path before renaming the asset directory into place.
func (s *Store) repairLegacyAssetFilePath(metadataPath string, record *assetRecord) error {
	assetDir := filepath.Dir(metadataPath)
	if record.ID == "" || filepath.Base(assetDir) != record.ID || record.File == "" {
		return nil
	}
	storedPath := filepath.FromSlash(record.File)
	if !filepath.IsLocal(storedPath) {
		return nil
	}
	currentPath := filepath.Join(s.root, storedPath)
	if info, err := os.Stat(currentPath); err == nil && info.Mode().IsRegular() {
		return nil
	}

	filename := filepath.Base(storedPath)
	if filename == "." || filename == ".." || filename == string(filepath.Separator) {
		return nil
	}
	fallbackPath := filepath.Join(assetDir, filename)
	relativeToAsset, err := filepath.Rel(assetDir, fallbackPath)
	if err != nil || relativeToAsset == ".." || strings.HasPrefix(relativeToAsset, ".."+string(os.PathSeparator)) || filepath.IsAbs(relativeToAsset) {
		return nil
	}
	info, err := os.Stat(fallbackPath)
	if err != nil || !info.Mode().IsRegular() {
		return nil
	}
	relativeToRoot, err := filepath.Rel(s.root, fallbackPath)
	if err != nil || !filepath.IsLocal(relativeToRoot) {
		return fmt.Errorf("resolve repaired asset path")
	}
	record.File = filepath.ToSlash(relativeToRoot)
	if err := writeJSONAtomic(metadataPath, *record); err != nil {
		return fmt.Errorf("persist repaired asset path: %w", err)
	}
	return nil
}

func (s *Store) GetSession(id string) (*uploadSession, error) {
	if !validID(id) {
		return nil, os.ErrNotExist
	}
	s.mu.Lock()
	handle, ok := s.sessions[id]
	if !ok {
		data, err := os.ReadFile(s.sessionStatePath(id))
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}
		var state sessionState
		if err := json.Unmarshal(data, &state); err != nil || state.ID != id {
			s.mu.Unlock()
			return nil, fmt.Errorf("read upload session %s: invalid session state", id)
		}
		handle = &uploadSession{state: state}
		s.sessions[id] = handle
	}
	s.mu.Unlock()

	handle.mu.Lock()
	defer handle.mu.Unlock()
	if !handle.state.Complete {
		if err := s.recoverCommittedAsset(handle); err != nil {
			return nil, err
		}
		if handle.state.Complete {
			_ = os.Remove(s.partPath(handle.state.ID))
		} else if err := s.cleanupInterruptedFinalize(handle); err != nil {
			return nil, err
		}
	}
	if err := s.reconcilePart(handle); err != nil {
		return nil, err
	}
	return handle, nil
}

func (s *Store) cleanupInterruptedFinalize(handle *uploadSession) error {
	capturedAt := handle.state.CapturedAt.UTC()
	parent := filepath.Join(s.root, "originals", capturedAt.Format("2006"), capturedAt.Format("01"))
	pattern := filepath.Join(parent, "."+handle.state.ID+"-*")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("find interrupted asset finalization: %w", err)
	}
	for _, match := range matches {
		if err := os.RemoveAll(match); err != nil {
			return fmt.Errorf("remove interrupted asset finalization: %w", err)
		}
	}
	return nil
}

func (s *Store) recoverCommittedAsset(handle *uploadSession) error {
	capturedAt := handle.state.CapturedAt.UTC()
	assetDir := filepath.Join(s.root, "originals", capturedAt.Format("2006"), capturedAt.Format("01"), handle.state.ID)
	record, err := readAssetRecord(filepath.Join(assetDir, "asset.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("recover committed asset %s: %w", handle.state.ID, err)
	}
	if record.ID != handle.state.ID {
		return fmt.Errorf("recover committed asset %s: mismatched asset id", handle.state.ID)
	}
	handle.state.Complete = true
	handle.state.StoredAssetID = record.ID
	relativeDir, err := filepath.Rel(s.root, assetDir)
	if err != nil {
		return fmt.Errorf("recover committed asset path: %w", err)
	}
	handle.state.AssetDirectory = filepath.ToSlash(relativeDir)
	handle.state.Offset = record.Size
	return s.persist(handle)
}

func (s *Store) reconcilePart(handle *uploadSession) error {
	if handle.state.Complete {
		return nil
	}
	info, err := os.Stat(s.partPath(handle.state.ID))
	if err != nil {
		return err
	}
	size := info.Size()
	if size > handle.state.Offset {
		file, err := os.OpenFile(s.partPath(handle.state.ID), os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		err = file.Truncate(handle.state.Offset)
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		return nil
	}
	if size < handle.state.Offset {
		handle.state.Offset = size
		return s.persist(handle)
	}
	return nil
}

func (s *Store) PartPath(id string) string {
	return s.partPath(id)
}

func (s *Store) Persist(handle *uploadSession) error {
	return s.persist(handle)
}

func (s *Store) ForgetSession(id string) {
	s.mu.Lock()
	delete(s.sessions, id)
	s.mu.Unlock()
}

func (s *Store) Finalize(handle *uploadSession) (Asset, error) {
	asset, _, err := s.FinalizeWithDuplicate(handle)
	return asset, err
}

func (s *Store) FinalizeWithDuplicate(handle *uploadSession) (Asset, bool, error) {
	if handle.state.Complete && handle.state.AssetDirectory != "" {
		directory := filepath.FromSlash(handle.state.AssetDirectory)
		if !filepath.IsLocal(directory) {
			return Asset{}, false, fmt.Errorf("invalid committed asset directory")
		}
		asset, err := s.readAssetInDirectory(filepath.Join(s.root, directory))
		return asset, handle.state.Duplicate, err
	}

	s.assetMu.Lock()
	defer s.assetMu.Unlock()

	capturedAt := handle.state.CapturedAt.UTC()
	yearMonth := filepath.Join(capturedAt.Format("2006"), capturedAt.Format("01"))
	parent := filepath.Join(s.root, "originals", yearMonth)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return Asset{}, false, fmt.Errorf("create asset directory: %w", err)
	}
	finalDir := filepath.Join(parent, handle.state.ID)
	if record, err := readAssetRecord(filepath.Join(finalDir, "asset.json")); err == nil {
		if record.ID != handle.state.ID {
			return Asset{}, false, fmt.Errorf("existing asset has mismatched id")
		}
		handle.state.Complete = true
		relativeDir, err := filepath.Rel(s.root, finalDir)
		if err != nil {
			return Asset{}, false, fmt.Errorf("resolve existing asset path: %w", err)
		}
		handle.state.AssetDirectory = filepath.ToSlash(relativeDir)
		handle.state.Offset = record.Size
		handle.state.StoredAssetID = record.ID
		if err := s.persist(handle); err != nil {
			return Asset{}, false, err
		}
		return record.Asset, false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return Asset{}, false, fmt.Errorf("read existing asset metadata: %w", err)
	}

	tempDir, err := os.MkdirTemp(parent, "."+handle.state.ID+"-")
	if err != nil {
		return Asset{}, false, fmt.Errorf("create temporary asset directory: %w", err)
	}
	filename := handle.state.ID + "_" + handle.state.Filename
	source := s.partPath(handle.state.ID)
	dest := filepath.Join(tempDir, filename)
	committed := false
	defer func() {
		if !committed {
			_ = os.RemoveAll(tempDir)
		}
	}()

	if err := os.Link(source, dest); err != nil {
		if copyErr := copyFile(source, dest); copyErr != nil {
			return Asset{}, false, fmt.Errorf("stage completed upload in asset store: hard link failed: %v; copy failed: %w", err, copyErr)
		}
	}
	file, err := os.Open(dest)
	if err != nil {
		return Asset{}, false, fmt.Errorf("open completed upload: %w", err)
	}
	info, statErr := file.Stat()
	sha256Hash := sha256.New()
	sha1Hash := sha1.New()
	_, hashErr := io.Copy(io.MultiWriter(sha256Hash, sha1Hash), file)
	closeErr := file.Close()
	if statErr != nil {
		return Asset{}, false, statErr
	}
	if hashErr != nil {
		return Asset{}, false, fmt.Errorf("hash completed upload: %w", hashErr)
	}
	if closeErr != nil {
		return Asset{}, false, closeErr
	}
	asset := Asset{
		ID:             handle.state.ID,
		DeviceID:       handle.state.DeviceID,
		DeviceAssetID:  handle.state.DeviceAssetID,
		AssetVersion:   handle.state.AssetVersion,
		ResourceType:   handle.state.ResourceType,
		Filename:       handle.state.Filename,
		MediaType:      handle.state.MediaType,
		CapturedAt:     handle.state.CapturedAt,
		FileModifiedAt: handle.state.FileModifiedAt,
		IsFavorite:     handle.state.IsFavorite,
		ICloudID:       handle.state.ICloudID,
		Latitude:       handle.state.Latitude,
		Longitude:      handle.state.Longitude,
		TimezoneOffset: handle.state.TimezoneOffset,
		SourceChecksum: handle.state.SourceChecksum,
		UploadedAt:     time.Now().UTC(),
		Size:           info.Size(),
		SHA256:         hex.EncodeToString(sha256Hash.Sum(nil)),
		SHA1:           hex.EncodeToString(sha1Hash.Sum(nil)),
	}
	key := makeIdempotencyKey(asset.DeviceID, asset.DeviceAssetID, asset.AssetVersion)
	if key != "" {
		if existing, ok := s.assetIndex[key]; ok && existing.SHA256 == asset.SHA256 {
			directory := filepath.Dir(existing.File)
			if !filepath.IsLocal(directory) {
				return Asset{}, false, fmt.Errorf("invalid stored duplicate asset path")
			}
			handle.state.Complete = true
			handle.state.Duplicate = true
			handle.state.StoredAssetID = existing.ID
			handle.state.AssetDirectory = filepath.ToSlash(directory)
			handle.state.Offset = existing.Size
			if err := s.persist(handle); err != nil {
				return Asset{}, false, err
			}
			_ = os.Remove(source)
			return existing.Asset, true, nil
		}
	}
	relativeFile, err := filepath.Rel(s.root, filepath.Join(finalDir, filename))
	if err != nil {
		return Asset{}, false, err
	}
	record := assetRecord{Asset: asset, File: filepath.ToSlash(relativeFile), IdempotencyKey: key}
	if err := writeJSONAtomic(filepath.Join(tempDir, "asset.json"), record); err != nil {
		return Asset{}, false, fmt.Errorf("write asset metadata: %w", err)
	}
	if err := os.Rename(tempDir, finalDir); err != nil {
		return Asset{}, false, fmt.Errorf("commit asset: %w", err)
	}
	committed = true
	handle.state.Complete = true
	relativeDir, err := filepath.Rel(s.root, finalDir)
	if err != nil {
		return Asset{}, false, fmt.Errorf("resolve committed asset path: %w", err)
	}
	handle.state.AssetDirectory = filepath.ToSlash(relativeDir)
	handle.state.Offset = asset.Size
	handle.state.StoredAssetID = asset.ID
	if key != "" {
		s.assetIndex[key] = record
	}
	if err := s.persist(handle); err != nil {
		return Asset{}, false, err
	}
	_ = os.Remove(source)
	return asset, false, nil
}

func copyFile(source, destination string) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(output, input)
	syncErr := output.Sync()
	closeErr := output.Close()
	return errors.Join(copyErr, syncErr, closeErr)
}

func (s *Store) ListAssets(limit int) ([]Asset, error) {
	if limit < 1 || limit > 1000 {
		limit = 200
	}
	root := filepath.Join(s.root, "originals")
	assets := make([]Asset, 0)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() != "asset.json" {
			return nil
		}
		record, err := readAssetRecord(path)
		if err != nil {
			return err
		}
		assets = append(assets, record.Asset)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list stored assets: %w", err)
	}
	sort.Slice(assets, func(i, j int) bool {
		return assets[i].UploadedAt.After(assets[j].UploadedAt)
	})
	if len(assets) > limit {
		assets = assets[:limit]
	}
	return assets, nil
}

func (s *Store) GetAsset(id string) (Asset, string, error) {
	if !validID(id) {
		return Asset{}, "", os.ErrNotExist
	}
	root := filepath.Join(s.root, "originals")
	var found assetRecord
	var filePath string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() || entry.Name() != id {
			return nil
		}
		record, err := readAssetRecord(filepath.Join(path, "asset.json"))
		if err != nil {
			return err
		}
		if record.ID != id {
			return fmt.Errorf("stored asset id does not match its directory")
		}
		storedPath := filepath.FromSlash(record.File)
		if !filepath.IsLocal(storedPath) {
			return fmt.Errorf("invalid stored asset path")
		}
		filePath = filepath.Join(s.root, storedPath)
		relativeToDir, err := filepath.Rel(path, filePath)
		if err != nil || relativeToDir == ".." || strings.HasPrefix(relativeToDir, ".."+string(os.PathSeparator)) || filepath.IsAbs(relativeToDir) {
			return fmt.Errorf("stored asset path escapes its directory")
		}
		found = record
		return filepath.SkipAll
	})
	if err != nil && err != filepath.SkipAll {
		return Asset{}, "", err
	}
	if filePath == "" {
		return Asset{}, "", os.ErrNotExist
	}
	return found.Asset, filePath, nil
}

func (s *Store) readAssetInDirectory(dir string) (Asset, error) {
	data, err := os.ReadFile(filepath.Join(dir, "asset.json"))
	if err != nil {
		return Asset{}, err
	}
	var record assetRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return Asset{}, err
	}
	return record.Asset, nil
}

func (s *Store) MarkForwardResult(id, immichAssetID string, forwardErr string) error {
	if !validID(id) {
		return os.ErrNotExist
	}
	s.assetMu.Lock()
	defer s.assetMu.Unlock()

	_, filePath, err := s.GetAsset(id)
	if err != nil {
		return err
	}
	metadataPath := filepath.Join(filepath.Dir(filePath), "asset.json")
	record, err := readAssetRecord(metadataPath)
	if err != nil {
		return fmt.Errorf("read stored asset status: %w", err)
	}
	if immichAssetID != "" {
		record.ImmichAssetID = immichAssetID
		record.ForwardedAt = time.Now().UTC()
		record.ForwardError = ""
		record.ForwardAttempts = 0
		record.ForwardRetryAt = time.Time{}
	} else {
		record.ForwardError = forwardErr
		record.ForwardAttempts++
		delay := time.Minute * time.Duration(1<<min(record.ForwardAttempts-1, 6))
		record.ForwardRetryAt = time.Now().UTC().Add(delay)
	}
	if err := writeJSONAtomic(metadataPath, record); err != nil {
		return fmt.Errorf("write stored asset status: %w", err)
	}
	if record.IdempotencyKey != "" {
		s.assetIndex[record.IdempotencyKey] = record
	}
	return nil
}

func (s *Store) PendingAssets(limit int) ([]PendingAsset, error) {
	if limit < 1 || limit > 1000 {
		limit = 100
	}
	root := filepath.Join(s.root, "originals")
	assets := make([]PendingAsset, 0)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() != "asset.json" {
			return nil
		}
		record, err := readAssetRecord(path)
		if err != nil {
			return err
		}
		if record.ImmichAssetID != "" || record.ForwardRetryAt.After(time.Now()) {
			return nil
		}
		storedPath := filepath.FromSlash(record.File)
		if !filepath.IsLocal(storedPath) {
			return fmt.Errorf("invalid stored asset path for %s", record.ID)
		}
		filePath := filepath.Join(s.root, storedPath)
		relativeToDir, err := filepath.Rel(filepath.Dir(path), filePath)
		if err != nil || relativeToDir == ".." || strings.HasPrefix(relativeToDir, ".."+string(os.PathSeparator)) || filepath.IsAbs(relativeToDir) {
			return fmt.Errorf("stored asset path escapes its directory for %s", record.ID)
		}
		assets = append(assets, PendingAsset{Asset: record.Asset, FilePath: filePath})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list pending assets: %w", err)
	}
	sort.Slice(assets, func(i, j int) bool {
		return assets[i].Asset.UploadedAt.Before(assets[j].Asset.UploadedAt)
	})
	if len(assets) > limit {
		assets = assets[:limit]
	}
	return assets, nil
}

func readAssetRecord(path string) (assetRecord, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return assetRecord{}, err
	}
	var record assetRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return assetRecord{}, err
	}
	return record, nil
}

func (s *Store) persist(handle *uploadSession) error {
	data, err := json.MarshalIndent(handle.state, "", "  ")
	if err != nil {
		return err
	}
	return writeBytesAtomic(s.sessionStatePath(handle.state.ID), data, 0o600)
}

func writeJSONAtomic(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return writeBytesAtomic(path, data, 0o600)
}

func writeBytesAtomic(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(dir, ".write-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(mode); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(tempName, path)
}

func (s *Store) sessionDir(id string) string {
	return filepath.Join(s.root, ".staging", id)
}

func (s *Store) sessionStatePath(id string) string {
	return filepath.Join(s.sessionDir(id), "session.json")
}

func (s *Store) partPath(id string) string {
	return filepath.Join(s.sessionDir(id), "data.part")
}

func newID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		id[0:4], id[4:6], id[6:8], id[8:10], id[10:16]), nil
}

func validID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for i, r := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if r != '-' {
				return false
			}
			continue
		}
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			return false
		}
	}
	return true
}

func makeIdempotencyKey(deviceID, deviceAssetID, assetVersion string) string {
	if strings.TrimSpace(deviceID) == "" || strings.TrimSpace(deviceAssetID) == "" {
		return ""
	}
	return deviceID + "\x00" + deviceAssetID + "\x00" + assetVersion
}
