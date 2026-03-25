package colmena

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// LocalBackend stores backups on the local filesystem.
type LocalBackend struct {
	dir string
}

// NewLocalBackend creates a backup backend that writes to the given directory.
func NewLocalBackend(dir string) (*LocalBackend, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("colmena: create backup dir: %w", err)
	}
	return &LocalBackend{dir: dir}, nil
}

func (b *LocalBackend) generationDir(generation string) string {
	return filepath.Join(b.dir, generation)
}

func (b *LocalBackend) WriteSnapshot(ctx context.Context, generation string, r io.Reader, size int64) error {
	genDir := b.generationDir(generation)
	if err := os.MkdirAll(genDir, 0755); err != nil {
		return err
	}

	// Write snapshot file.
	snapPath := filepath.Join(genDir, "snapshot.db")
	f, err := os.Create(snapPath)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	// Write generation metadata.
	meta := Generation{
		ID:        generation,
		CreatedAt: time.Now().UTC(),
	}
	metaData, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(genDir, "meta.json"), metaData, 0644)
}

func (b *LocalBackend) WriteWAL(ctx context.Context, generation string, r io.Reader, size int64) error {
	genDir := b.generationDir(generation)
	if err := os.MkdirAll(genDir, 0755); err != nil {
		return err
	}

	// Write WAL file (replace existing).
	walPath := filepath.Join(genDir, "wal.db")
	f, err := os.Create(walPath)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func (b *LocalBackend) Generations(ctx context.Context) ([]Generation, error) {
	entries, err := os.ReadDir(b.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var gens []Generation
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		metaPath := filepath.Join(b.dir, entry.Name(), "meta.json")
		metaData, err := os.ReadFile(metaPath)
		if err != nil {
			continue // skip entries without metadata
		}
		var gen Generation
		if err := json.Unmarshal(metaData, &gen); err != nil {
			continue
		}
		gens = append(gens, gen)
	}

	// Sort newest first.
	sort.Slice(gens, func(i, j int) bool {
		return gens[i].CreatedAt.After(gens[j].CreatedAt)
	})
	return gens, nil
}

func (b *LocalBackend) ReadSnapshot(ctx context.Context, generation string) (io.ReadCloser, error) {
	snapPath := filepath.Join(b.generationDir(generation), "snapshot.db")
	return os.Open(snapPath)
}

func (b *LocalBackend) ReadWAL(ctx context.Context, generation string) (io.ReadCloser, error) {
	walPath := filepath.Join(b.generationDir(generation), "wal.db")
	return os.Open(walPath)
}

func (b *LocalBackend) Close() error {
	return nil
}
