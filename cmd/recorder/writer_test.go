package main

import (
	"bufio"
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRotatingWriterRotatesAndProducesValidGzip(t *testing.T) {
	dir := t.TempDir()
	w := newRotatingWriter(dir)

	hour0 := time.Date(2026, 9, 27, 10, 30, 0, 0, time.UTC)
	hour1 := time.Date(2026, 9, 27, 11, 5, 0, 0, time.UTC)

	if err := w.WriteLine(hour0, []byte(`{"t":"tick","d":{"a":"x"}}`)); err != nil {
		t.Fatalf("write hour0: %v", err)
	}
	if err := w.WriteLine(hour0, []byte(`{"t":"alert","d":{"a":"y"}}`)); err != nil {
		t.Fatalf("write hour0 line2: %v", err)
	}
	if err := w.WriteLine(hour1, []byte(`{"t":"tick","d":{"a":"z"}}`)); err != nil {
		t.Fatalf("write hour1: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	path0 := filepath.Join(dir, "2026-09-27", "10.ndjson.gz")
	path1 := filepath.Join(dir, "2026-09-27", "11.ndjson.gz")

	lines0 := readGzipLines(t, path0)
	if len(lines0) != 2 {
		t.Fatalf("hour0 file: got %d lines, want 2: %v", len(lines0), lines0)
	}
	lines1 := readGzipLines(t, path1)
	if len(lines1) != 1 {
		t.Fatalf("hour1 file: got %d lines, want 1: %v", len(lines1), lines1)
	}
}

func readGzipLines(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gzip reader %s: %v", path, err)
	}
	defer func() { _ = gz.Close() }()
	var lines []string
	sc := bufio.NewScanner(gz)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}
	return lines
}
