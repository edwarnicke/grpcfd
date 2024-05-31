package test

import (
	"context"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/edwarnicke/grpcfd"
	"github.com/pkg/errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	pb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	socketPath       = "/tmp/grpcfd_test.sock"
	filePath         = "/tmp/grpcfd_server_receive_test.txt"
	filePath2        = "/tmp/grpcfd_server_send_test.txt"
	expectedContent  = "Written by the client...\nSeen by server...\nWritten by the client...\nSeen by server...\nWritten by the client...\nSeen by server...\n"
	expectedContent2 = "Written by the server...\nSeen by client...\nWritten by the server...\nSeen by client...\nWritten by the server...\nSeen by client...\n"
)

type fdServer struct {
	pb.UnimplementedHealthServer
}

func (s *fdServer) Check(ctx context.Context, in *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	recv, ok := grpcfd.FromContext(ctx)
	if !ok {
		return nil, errors.New("No grpcfd receiver is available")
	}

	if in.GetService() != "" {
		fileCh, err := recv.RecvFileByURL(in.GetService())
		if err != nil {
			return nil, errors.New("Failed to receive FD")
		}
		file := <-fileCh

		_, err = file.Seek(0, 0)
		if err != nil {
			return nil, err
		}

		defer func() {
			err = file.Close()
			if err != nil {
				log.Printf("Error occurred while closing file: %v", err)
			}
		}()

		_, err = file.WriteString("Seen by server...\n")
		if err != nil {
			return nil, err
		}

		return &pb.HealthCheckResponse{Status: pb.HealthCheckResponse_SERVING}, nil
	} else {
		file, err := os.OpenFile(filePath2, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
		if err != nil {
			return nil, errors.New("Failed to open file")
		}

		defer func() {
			err = file.Close()
			if err != nil {
				log.Printf("Error occurred while closing file: %v", err)
			}
		}()

		_, err = file.WriteString("Written by the server...\n")
		if err != nil {
			return nil, err
		}

		errCh := recv.SendFD(file.Fd())
		select {
		case err := <-errCh:
			if err != nil {
				return nil, err
			}
		default:
		}

		return &pb.HealthCheckResponse{Status: pb.HealthCheckResponse_SERVING}, nil
	}
}

func startServer(stopCh, doneCh chan struct{}, errCh chan<- error) {
	if err := os.RemoveAll(socketPath); err != nil {
		errCh <- err
	}

	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		errCh <- err
	}

	defer func() {
		err = syscall.Unlink(socketPath)
		if err != nil {
			log.Printf("Error occurred while unlinking: %v", err)
		}
	}()

	var creds credentials.TransportCredentials
	grpcServer := grpc.NewServer(grpc.Creds(grpcfd.TransportCredentials(creds)))
	pb.RegisterHealthServer(grpcServer, &fdServer{})

	go func() {
		<-stopCh
		grpcServer.GracefulStop()
		close(doneCh)
	}()

	errCh <- grpcServer.Serve(lis)
}

func startClient(stopCh, doneCh chan struct{}) (credentials.PerRPCCredentials, grpcfd.FDTransceiver, pb.HealthClient, error) {
	perRPCCredentials := grpcfd.PerRPCCredentials(nil)
	sender, ok := grpcfd.FromPerRPCCredentials(perRPCCredentials)
	if !ok {
		return nil, nil, nil, errors.New("No grpcfd sender is available")
	}

	conn, err := grpc.Dial("unix:"+socketPath,
		grpc.WithTransportCredentials(grpcfd.TransportCredentials(insecure.NewCredentials())),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		grpcfd.WithChainUnaryInterceptor(),
		grpcfd.WithChainStreamInterceptor(),
	)
	if err != nil {
		return nil, nil, nil, errors.Errorf("Failed to connect: %v", err)
	}

	go func() {
		<-stopCh
		err = conn.Close()
		if err != nil {
			log.Printf("Error occurred while closing connection: %v", err)
		}
		close(doneCh)
	}()

	client := pb.NewHealthClient(conn)

	return perRPCCredentials, sender, client, nil
}

func clientSendFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	perRPCCredentials, sender, client, err := startClient(stopCh, doneCh)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(stopCh)
		<-doneCh
	}()

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("Failed to open file %v", filePath)
	}

	defer func() {
		err = file.Close()
		if err != nil {
			t.Fatalf("Error occurred while closing file: %v", err)
		}
	}()

	for i := 0; i < 3; i++ {
		_, err := file.WriteString("Written by the client...\n")
		if err != nil {
			t.Fatalf("Error occurred: %v", err)
		}

		errCh := sender.SendFD(file.Fd())

		select {
		case err = <-errCh:
			if err != nil {
				t.Fatalf("Error occurred: %v", err)
			}
		default:
		}

		inodeURL, err := grpcfd.FilenameToURL(filePath)
		if err != nil {
			t.Fatalf("Error occurred: %v", err)
		}
		fd := &pb.HealthCheckRequest{Service: inodeURL.String()}
		_, err = client.Check(ctx, fd, grpc.PerRPCCredentials(perRPCCredentials))
		if err != nil {
			t.Fatalf("Error occurred: %v", err)
		}
	}
}

func clientRecvFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	perRPCCredentials, recv, client, err := startClient(stopCh, doneCh)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(stopCh)
		<-doneCh
	}()

	for i := 0; i < 3; i++ {
		_, err := client.Check(ctx, &pb.HealthCheckRequest{Service: ""}, grpc.PerRPCCredentials(perRPCCredentials))
		if err != nil {
			t.Fatalf("Error occurred: %v", err)
		}

		url, err := grpcfd.FilenameToURL(filePath2)
		if err != nil {
			t.Fatalf("Error occurred: %v", err)
		}
		fileCh, err := recv.RecvFileByURL(url.String())
		if err != nil {
			t.Fatalf("Error occurred: %v", err)
		}
		file := <-fileCh

		_, err = file.Seek(0, 0)
		if err != nil {
			t.Fatalf("Error occurred: %v", err)
		}

		_, err = io.ReadAll(file)
		if err != nil {
			t.Fatalf("Error occurred: %v", err)
		}

		defer func() {
			err = file.Close()
			if err != nil {
				t.Fatalf("Error occurred while closing file: %v", err)
			}
		}()

		_, err = file.WriteString("Seen by client...\n")
		if err != nil {
			t.Fatalf("Error occurred: %v", err)
		}
	}
}

func startServerForTest(t *testing.T) (stopCh, doneCh chan struct{}) {
	t.Log("Starting test")
	errCh := make(chan error)
	stopCh = make(chan struct{})
	doneCh = make(chan struct{})

	go startServer(stopCh, doneCh, errCh)
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Server error: %v", err)
		}
	case <-time.After(2 * time.Second):
	}
	t.Log("Server started successfully")

	time.Sleep(2 * time.Second)

	return stopCh, doneCh
}

func compareTestFiles(t *testing.T, filePath, expectedContent string) {
	file, err := os.Open(filepath.Clean(filePath))
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	defer func() {
		err = file.Close()
		if err != nil {
			t.Fatalf("Error occurred while closing file: %v", err)
		}
	}()

	readBuffer, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("Error while reading file: %v", err)
	}
	if string(readBuffer) != expectedContent {
		t.Fatalf("File content does not match. Got: \n%s", string(readBuffer))
	}

	if err := os.Remove(filePath); err != nil {
		t.Fatalf("Failed to remove file: %v", err)
	}
}

func TestServerRecvFileByURL(t *testing.T) {
	stopCh, doneCh := startServerForTest(t)

	clientSendFile(t)
	t.Log("Client done")

	close(stopCh)
	<-doneCh

	compareTestFiles(t, filePath, expectedContent)
	t.Log("Cleanup complete")
}

func TestClientRecvFileByURL(t *testing.T) {
	stopCh, doneCh := startServerForTest(t)

	clientRecvFile(t)
	t.Log("Client done")

	close(stopCh)
	<-doneCh

	compareTestFiles(t, filePath2, expectedContent2)
	t.Log("Cleanup complete")
}
