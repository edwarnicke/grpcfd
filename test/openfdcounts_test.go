package test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	health "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/edwarnicke/grpcfd"
	"github.com/edwarnicke/grpcfd/test/internal/mock"
	"github.com/edwarnicke/grpcfd/test/internal/openfdcount"

	"github.com/stretchr/testify/assert"
)

const (
	socketname = "/tmp/abracadbra.sock"
	filename   = "/tmp/randomfiletosend.txt"
	N          = 16
)

type callbackType = func(cvr grpcfd.FDTransceiver, filename string) (*health.HealthCheckResponse, error)

func keys[K comparable, V any](m map[K]V) []K {
	ks := make([]K, 0)
	for key := range m {
		ks = append(ks, key)
	}
	return ks
}

func assertFdCount(t *testing.T, filename string, ofdcount int) {
	t.Helper()
	cnt, err := openfdcount.OpenFDCount(filename)
	if err != nil {
		t.Error(err)
		return
	}
	if cnt != ofdcount {
		t.Errorf("open fd count excpected: %d, got: %d\n", ofdcount, cnt)
	}
}

func fatalOnErr(t *testing.T, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
}

func touch(filename string) error {
	filename = filepath.Clean(filename)
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDONLY, 0o600)
	if err != nil {
		return err
	}
	return f.Close()
}

func makeMockSrv(t *testing.T, openfdcnt int) callbackType {
	return func(cvr grpcfd.FDTransceiver, filename string) (*health.HealthCheckResponse, error) {
		url, err := grpcfd.FilenameToURL(filename)
		assert.NoError(t, err)
		ch, err := cvr.RecvFileByURL(url.String())
		assert.NoError(t, err)

		assertFdCount(t, filename, openfdcnt+2)

		file := <-ch
		assert.NoError(t, file.Close())

		assertFdCount(t, filename, openfdcnt+1)

		return &health.HealthCheckResponse{Status: health.HealthCheckResponse_SERVING}, nil
	}
}

func setupFiles(t *testing.T, files []string) func() {
	for _, file := range files {
		t.Logf("Creating file: %s\n", file)
		fatalOnErr(t, touch(file))
	}

	return func() {
		for _, file := range files {
			t.Logf("Removing file: %s\n", file)
			assert.NoError(t, os.Remove(file))
		}
	}
}

func setupServer(t *testing.T, cb callbackType) (server *grpc.Server, cleanup func()) {
	t.Log("Setting up server")
	m := mock.NewMockServer(cb)
	lis, err := net.Listen("unix", socketname)
	fatalOnErr(t, err)
	var creds credentials.TransportCredentials
	srv := grpc.NewServer(grpc.Creds(grpcfd.TransportCredentials(creds)))
	health.RegisterHealthServer(srv, m)

	t.Log("Start Serve")
	go func() {
		err := srv.Serve(lis)
		assert.NoError(t, err)
	}()
	t.Log("Returning cleanup")
	return srv, func() {
		err := os.Remove(socketname)
		if err != nil {
			switch v := err.(type) {
			case *os.PathError: // it looks like grpc.Server.Stop() also removes the socket, so I'm checking ENOENT
				if v.Err == syscall.ENOENT {
					return
				}
			default:
				t.Fatal(err)
			}
		}
	}
}

func TestTruth(t *testing.T) {
	assert.NoError(t, nil)
	assert.Equal(t, 0, 0)
	fatalOnErr(t, nil)
	t.Cleanup(setupFiles(t, []string{filename}))
	assertFdCount(t, filename, 0)
}

func TestTouch(t *testing.T) {
	_, err := os.Stat(filename)
	assert.Error(t, err)
	err = touch(filename)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, os.Remove(filename)) })
	_, err = os.Stat(filename)
	assert.NoError(t, err)

	_, err = grpcfd.FilenameToURL(filename)
	assert.NoError(t, err)
}

func TestSendFile(t *testing.T) {
	srv, cleanup := setupServer(t, makeMockSrv(t, 1))
	t.Cleanup(func() {
		cleanup()
		srv.Stop()
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDONLY, 0o600)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, file.Close())
		assertFdCount(t, filename, 1)
	}()

	assertFdCount(t, filename, 1)

	cli := mock.NewClient(socketname, func(sender grpcfd.FDTransceiver) (<-chan error, string) {
		errCh := sender.SendFile(file)
		return errCh, filename
	})
	err = cli.Dial()
	assert.NoError(t, err)

	err = cli.Send(ctx)
	assert.NoError(t, err)
}

func TestSendFilename(t *testing.T) {
	t.Cleanup(setupFiles(t, []string{filename}))
	srv, cleanup := setupServer(t, makeMockSrv(t, 0))
	t.Cleanup(func() {
		cleanup()
		srv.Stop()
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	cli := mock.NewClient(socketname, func(sender grpcfd.FDTransceiver) (<-chan error, string) {
		errCh := sender.SendFilename(filename)
		assertFdCount(t, filename, 1)
		return errCh, filename
	})

	err := cli.Dial()
	assert.NoError(t, err)

	err = cli.Send(ctx)
	assert.NoError(t, err)

	// after Send the only fd open should be in srv
	assertFdCount(t, filename, 1)
}

func TestSendFail(t *testing.T) {
	t.Cleanup(setupFiles(t, []string{filename}))
	srv, cleanup := setupServer(t, makeMockSrv(t, 0))
	t.Cleanup(cleanup)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	{
		cli := mock.NewClient(socketname, func(sender grpcfd.FDTransceiver) (<-chan error, string) {
			errCh := sender.SendFilename(filename)
			assertFdCount(t, filename, 1)
			return errCh, filename
		})

		err := cli.Dial()
		assert.NoError(t, err)

		srv.Stop()
		resp := cli.Send(ctx)
		assert.NotNil(t, resp)

		assertFdCount(t, filename, 1)
	}
	runtime.GC()
	assertFdCount(t, filename, 0)
}

func TestServerSend(t *testing.T) {
	t.Cleanup(setupFiles(t, []string{filename}))
	srv, cleanup := setupServer(t, func(cvr grpcfd.FDTransceiver, _ string) (*health.HealthCheckResponse, error) {
		assertFdCount(t, filename, 0)
		errCh := cvr.SendFilename(filename)
		select {
		case err := <-errCh:
			return nil, err
		default:
		}
		assertFdCount(t, filename, 1)
		return &health.HealthCheckResponse{Status: health.HealthCheckResponse_SERVING}, nil
	})
	t.Cleanup(cleanup)
	defer func() { // should be zero after srv is closed
		srv.Stop()
		assertFdCount(t, filename, 0)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	cli := mock.NewClient(socketname, nil)
	err := cli.Dial()
	fatalOnErr(t, err)
	defer func() {
		assert.NoError(t, os.Remove(socketname))
	}()

	fileCh, err := cli.Recv(ctx, filename)
	assert.NoError(t, err)

	f := <-fileCh
	defer func() {
		assert.NoError(t, f.Close())
		assertFdCount(t, filename, 1)
	}()

	assertFdCount(t, filename, 2)
}

func TestMultiClient(t *testing.T) {
	filemap := map[string]int{
		"/tmp/testfilename1.txt": 0,
		"/tmp/testfilename2.txt": 0,
		"/tmp/testfilename3.txt": 0,
	}
	t.Cleanup(setupFiles(t, keys(filemap)))
	srv, cleanup := setupServer(t, func(cvr grpcfd.FDTransceiver, filename string) (*health.HealthCheckResponse, error) {
		url, err := grpcfd.FilenameToURL(filename)
		fatalOnErr(t, err)
		ch, err := cvr.RecvFileByURL(url.String())
		fatalOnErr(t, err)

		file := <-ch
		assert.NoError(t, file.Close())

		assertFdCount(t, filename, 1)

		return &health.HealthCheckResponse{Status: health.HealthCheckResponse_SERVING}, nil
	})
	t.Cleanup(func() {
		cleanup()
		srv.Stop()
	})

	fun := func() {
		for filepath := range filemap {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()

			err := touch(filepath)
			assert.NoError(t, err)

			cli := mock.NewClient(socketname, func(sender grpcfd.FDTransceiver) (<-chan error, string) {
				errCh := sender.SendFilename(filepath)
				filemap[filepath]++
				return errCh, filepath
			})
			err = cli.Dial()
			assert.NoError(t, err)
			defer func() {
				assert.NoError(t, cli.Close())
			}()

			_ = cli.Send(ctx)
			assertFdCount(t, filepath, 1)
		}
	}
	for i := 0; i < N; i++ {
		fun()
	}
	for filepath := range filemap {
		assertFdCount(t, filepath, 0)
	}
}

func TestSameFileOverSameConn(t *testing.T) {
	t.Cleanup(setupFiles(t, []string{filename}))
	srv, cleanup := setupServer(t, func(cvr grpcfd.FDTransceiver, filename string) (*health.HealthCheckResponse, error) {
		url, err := grpcfd.FilenameToURL(filename)
		assert.NoError(t, err)
		ch, err := cvr.RecvFileByURL(url.String())
		assert.NoError(t, err)

		file := <-ch
		assert.NoError(t, file.Close())

		assertFdCount(t, filename, 1)

		return &health.HealthCheckResponse{Status: health.HealthCheckResponse_SERVING}, nil
	})

	t.Cleanup(func() {
		cleanup()
		srv.Stop()
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	err := touch(filename)
	assert.NoError(t, err)

	cli := mock.NewClient(socketname, func(sender grpcfd.FDTransceiver) (<-chan error, string) {
		errCh := sender.SendFilename(filename)
		return errCh, filename
	})
	err = cli.Dial()
	fatalOnErr(t, err)

	for i := 0; i < N; i++ {
		err = cli.Send(ctx)
		fatalOnErr(t, err)
		assertFdCount(t, filename, 1)
	}

	assert.NoError(t, cli.Close())
	assertFdCount(t, filename, 0)
}
