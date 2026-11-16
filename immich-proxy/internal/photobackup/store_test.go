package photobackup

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSaveUploadStoresReopenableOriginal(t *testing.T) {
	root := t.TempDir()
	store, err := NewStore(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}

	content := []byte("synthetic test photo")
	asset, duplicate, err := store.SaveUpload(testUploadMetadata(), bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if duplicate {
		t.Fatal("first upload unexpectedly marked duplicate")
	}

	_, originalPath, err := store.GetAsset(asset.ID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(originalPath)
	if err != nil {
		t.Fatalf("reopen saved original at %q: %v", originalPath, err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("reopened original = %q, want %q", got, content)
	}

	pending, err := store.PendingAssets(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending asset count = %d, want 1", len(pending))
	}
	if _, err := os.Stat(pending[0].FilePath); err != nil {
		t.Fatalf("pending original path %q is not readable: %v", pending[0].FilePath, err)
	}
}

func TestNewStoreRepairsLegacyStagingPath(t *testing.T) {
	root := t.TempDir()
	store, err := NewStore(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	asset, _, err := store.SaveUpload(testUploadMetadata(), bytes.NewReader([]byte("legacy original")))
	if err != nil {
		t.Fatal(err)
	}

	metadataPath := filepath.Join(root, "originals", asset.CapturedAt.Format("2006"), asset.CapturedAt.Format("01"), asset.ID, "asset.json")
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	var record assetRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	filename := filepath.Base(filepath.FromSlash(record.File))
	legacyPath := filepath.Join("originals", asset.CapturedAt.Format("2006"), asset.CapturedAt.Format("01"), ".legacy-staging", filename)
	record.File = filepath.ToSlash(legacyPath)
	if err := writeJSONAtomic(metadataPath, record); err != nil {
		t.Fatal(err)
	}

	reopenedStore, err := NewStore(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	_, originalPath, err := reopenedStore.GetAsset(asset.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(originalPath); err != nil {
		t.Fatalf("repaired original path %q is not readable: %v", originalPath, err)
	}
	if strings.Contains(originalPath, ".legacy-staging") {
		t.Fatalf("legacy staging path was not repaired: %q", originalPath)
	}

	pending, err := reopenedStore.PendingAssets(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].FilePath != originalPath {
		t.Fatalf("pending assets = %#v, want repaired path %q", pending, originalPath)
	}
}

func testUploadMetadata() UploadMetadata {
	capturedAt := time.Date(2024, time.March, 5, 12, 30, 0, 0, time.UTC)
	return UploadMetadata{
		DeviceID:       "test-device",
		DeviceAssetID:  "test-asset-001",
		ResourceType:   "IMAGE",
		Filename:       "synthetic-test.jpg",
		MediaType:      "image/jpeg",
		CapturedAt:     capturedAt,
		FileModifiedAt: capturedAt,
	}
}
