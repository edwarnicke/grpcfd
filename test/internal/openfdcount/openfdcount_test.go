package openfdcount

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ---------------------------------------------------------------------------//
const (
	testFilename = "/tmp/myrandomtestfile.txt"
	N            = 10
)

// ---------------------------------------------------------------------------/

func touch(filename string) error {
	filename = filepath.Clean(filename)
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDONLY, 0o600)
	if err != nil {
		return err
	}
	return f.Close()
}

func TestGetCount(t *testing.T) {
	f, err := os.OpenFile(testFilename, os.O_CREATE|os.O_RDONLY, 0o600)
	t.Cleanup(func() {
		assert.NoError(t, os.Remove(testFilename))
	})
	defer func() {
		assert.NoError(t, f.Close())
	}()
	assert.NoError(t, err)

	f2, err := os.OpenFile(testFilename, os.O_CREATE|os.O_RDONLY, 0o600)
	defer func() {
		assert.NoError(t, f2.Close())
	}()
	assert.NoError(t, err)

	opencount, err := OpenFDCount(testFilename)
	assert.NoError(t, err)
	assert.Equal(t, 2, opencount, "Opencount should be 2 since we opened the file two times.")
}

func TestGetCountNoFile(t *testing.T) {
	_, err := OpenFDCount(testFilename)
	assert.NotNil(t, err)
}

func TestGetCountNoOpen(t *testing.T) {
	err := touch(testFilename)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, os.Remove(testFilename)) })

	opencount, err := OpenFDCount(testFilename)
	assert.NoError(t, err)
	assert.Equal(t, 0, opencount)
}

func TestGetCountMultipleProcesses(t *testing.T) {
	f, err := os.OpenFile(testFilename, os.O_CREATE|os.O_RDONLY, 0o600)
	assert.NoError(t, err)
	err = f.Sync()
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, f.Close())
		assert.NoError(t, os.Remove(testFilename))
	})
	opencount, err := OpenFDCount(testFilename)
	assert.NoError(t, err)
	assert.Equal(t, 1, opencount)

	cmd := exec.Command("tail", "-f", testFilename)
	err = cmd.Start()
	if err != nil {
		t.Fatal(err)
	}
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, cmd.Process.Kill())
		er := cmd.Wait()
		switch e := er.(type) {
		case *exec.ExitError:
			// since we issued a kill signal the wait should return -1 exit code according to https://pkg.go.dev/os#Process.Wait
			// its a bit weird though as its also the same return value for a process that has not exited
			assert.Equal(t, -1, e.ProcessState.ExitCode())
		default:
			t.Error(er)
		}
	}()

	opencount, err = OpenFDCount(testFilename)
	assert.NoError(t, err)
	assert.Equal(t, 2, opencount)

	f2, err := os.OpenFile(testFilename, os.O_CREATE|os.O_RDONLY, 0o600)
	assert.NoError(t, err)
	opencount, err = OpenFDCount(testFilename)
	assert.NoError(t, err)
	assert.Equal(t, 3, opencount)
	assert.NoError(t, f2.Close())
	opencount, err = OpenFDCount(testFilename)
	assert.NoError(t, err)
	assert.Equal(t, 2, opencount)
}

func TestGetCountN(t *testing.T) {
	fileArray := make([]*os.File, N)
	t.Cleanup(func() {
		assert.NoError(t, os.Remove(testFilename))
	})
	for i := 0; i < N; i++ {
		f, err := os.OpenFile(testFilename, os.O_CREATE|os.O_RDONLY, 0o600)
		assert.NoError(t, err)
		fileArray[i] = f
		opencount, err := OpenFDCount(testFilename)
		assert.NoError(t, err)
		t.Logf("after %d open(s) opencount of %s: %d\n", i+1, testFilename, opencount)
		assert.Equal(t, i+1, opencount)
	}
	for i := 0; i < N; i++ {
		assert.NoError(t, fileArray[i].Close())
		opencount, err := OpenFDCount(testFilename)
		assert.NoError(t, err)
		t.Logf("after %d close(s) opencount of %s: %d\n", i+1, testFilename, opencount)
		assert.Equal(t, N-i-1, opencount)
	}
}
