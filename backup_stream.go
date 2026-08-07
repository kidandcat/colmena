package colmena

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// copyFileGzip streams src → gzip(dst) with a fixed buffer so multi-GB
// databases never sit fully in RAM. Returns the compressed byte size.
func copyFileGzip(srcPath, dstPath string) (int64, error) {
	in, err := os.Open(srcPath)
	if err != nil {
		return 0, err
	}
	defer in.Close()

	out, err := os.Create(dstPath)
	if err != nil {
		return 0, err
	}
	gw := gzip.NewWriter(out)
	buf := make([]byte, 256<<10) // 256 KiB
	if _, err := io.CopyBuffer(gw, in, buf); err != nil {
		gw.Close()
		out.Close()
		os.Remove(dstPath)
		return 0, err
	}
	if err := gw.Close(); err != nil {
		out.Close()
		os.Remove(dstPath)
		return 0, err
	}
	if err := out.Close(); err != nil {
		os.Remove(dstPath)
		return 0, err
	}
	st, err := os.Stat(dstPath)
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

// copyRange writes [start, start+n) from srcPath into dstPath (raw bytes).
func copyRange(srcPath, dstPath string, start, n int64) error {
	if n < 0 {
		return fmt.Errorf("colmena: negative copy length")
	}
	in, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer in.Close()
	if _, err := in.Seek(start, io.SeekStart); err != nil {
		return err
	}
	out, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	buf := make([]byte, 256<<10)
	remaining := n
	for remaining > 0 {
		toRead := len(buf)
		if int64(toRead) > remaining {
			toRead = int(remaining)
		}
		nr, err := io.ReadFull(in, buf[:toRead])
		if err != nil {
			out.Close()
			os.Remove(dstPath)
			return err
		}
		if _, err := out.Write(buf[:nr]); err != nil {
			out.Close()
			os.Remove(dstPath)
			return err
		}
		remaining -= int64(nr)
	}
	if err := out.Close(); err != nil {
		os.Remove(dstPath)
		return err
	}
	return nil
}

// spoolDir returns <dataDir>/.colmena-spool/<dbName>.
func spoolDir(dbPath, dbName string) string {
	return filepath.Join(filepath.Dir(dbPath), ".colmena-spool", dbName)
}

// spoolRawPath is the on-disk name for one pending WAL segment (uncompressed).
func spoolRawPath(dir string, gen string, index, offset int64) string {
	return filepath.Join(dir, fmt.Sprintf("%s_%016x_%016x.raw", gen, index, offset))
}
