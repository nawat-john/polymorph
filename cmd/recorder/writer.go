package main

import (
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// rotatingWriter appends NDJSON lines to gzip-compressed files under
// dir/YYYY-MM-DD/HH.ndjson.gz, opening a new file whenever the wall-clock
// hour changes (design-plan.md section 4.4). Not safe for concurrent use -
// the recorder's single consume loop is the only writer.
type rotatingWriter struct {
	dir    string
	bucket string // dir-relative path of the currently open file, without extension
	file   *os.File
	gz     *gzip.Writer
}

func newRotatingWriter(dir string) *rotatingWriter {
	return &rotatingWriter{dir: dir}
}

// WriteLine appends one JSON line (a trailing newline is added) to the file
// for t's hour, rotating first if t falls in a different hour than the
// currently open file.
func (w *rotatingWriter) WriteLine(t time.Time, line []byte) error {
	bucket := filepath.Join(t.Format("2006-01-02"), t.Format("15"))
	if bucket != w.bucket {
		if err := w.rotate(bucket); err != nil {
			return err
		}
	}
	if _, err := w.gz.Write(line); err != nil {
		return err
	}
	_, err := w.gz.Write([]byte("\n"))
	return err
}

// Flush pushes buffered compressed data to disk without closing the file, so
// a crash between rotations loses at most the records written since the
// last flush (design-plan.md 4.4 does not require crash-exact durability;
// this is a pragmatic middle ground - see main.go's flush-per-poll-batch
// call site).
func (w *rotatingWriter) Flush() error {
	if w.gz == nil {
		return nil
	}
	if err := w.gz.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Close finalizes the gzip stream (writes its footer) and closes the file.
func (w *rotatingWriter) Close() error {
	if w.gz == nil {
		return nil
	}
	gzErr := w.gz.Close()
	fileErr := w.file.Close()
	w.gz, w.file = nil, nil
	if gzErr != nil {
		return gzErr
	}
	return fileErr
}

func (w *rotatingWriter) rotate(bucket string) error {
	if err := w.Close(); err != nil {
		return fmt.Errorf("close previous replay file: %w", err)
	}
	path := filepath.Join(w.dir, bucket+".ndjson.gz")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(path), err)
	}
	// O_APPEND: a restart resumes the same hour's file rather than
	// overwriting it. Appending raw bytes to an existing gzip file produces
	// a valid "multistream" gzip (concatenated gzip members), which
	// compress/gzip's Reader decodes transparently (its default
	// Multistream(true)) and so does gunzip/zcat.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	w.file = f
	w.gz = gzip.NewWriter(f)
	w.bucket = bucket
	return nil
}
