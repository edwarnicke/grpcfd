package test

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/edwarnicke/grpcfd"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/edwarnicke/grpcfd/test/proto"
)

const (
	socketPath       = "/tmp/grpcfd_test.sock"
	filePath         = "/tmp/grpcfd_server_receive_test.txt"
	filePath2        = "/tmp/grpcfd_server_send_test.txt"
	expectedContent  = "Written by the client...\nSeen by server...\nWritten by the client...\nSeen by server...\nWritten by the client...\nSeen by server...\n"
	expectedContent2 = "Written by the server...\nSeen by client...\nWritten by the server...\nSeen by client...\nWritten by the server...\nSeen by client...\n"
)

type fdServer struct {
	pb.UnimplementedFDServiceServer
}

func (s *fdServer) ServerReceive(ctx context.Context, in *pb.FD) (*pb.FD, error) {
	recv, ok := grpcfd.FromContext(ctx)
	if !ok {
		return nil, errors.New("No grpcfd receiver is available")
	}

	fileCh, err := recv.RecvFileByURL(in.GetName())
	if err != nil {
		return nil, errors.New("Failed to receive FD")
	}
	file := <-fileCh
	file.Seek(0, 0)
	defer file.Close()

	_, err = file.WriteString("Seen by server...\n")
	if err != nil {
		return nil, err
	}

	return in, nil
}

func (s *fdServer) ServerSend(ctx context.Context, in *pb.FD) (*pb.FD, error) {
	sender, ok := grpcfd.FromContext(ctx)
	if !ok {
		return nil, errors.New("No grpcfd sender is available")
	}
	file, err := os.OpenFile(filePath2, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, errors.New("Failed to open file")
	}
	defer file.Close()

	file.WriteString("Written by the server...\n")
	errCh := sender.SendFD(file.Fd())
	select {
	case err := <-errCh:
		if err != nil {
			return nil, err
		}
	default:
	}

	inodeURL, err := grpcfd.FilenameToURL(filePath2)
	if err != nil {
		return nil, err
	}
	return &pb.FD{Name: inodeURL.String()}, nil
}

func startServer(stopCh chan struct{}, doneCh chan struct{}, errCh chan<- error) {
	if err := os.RemoveAll(socketPath); err != nil {
		errCh <- err
	}

	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		errCh <- err
	}
	defer syscall.Unlink(socketPath)

	var creds credentials.TransportCredentials
	grpcServer := grpc.NewServer(grpc.Creds(grpcfd.TransportCredentials(creds)))
	pb.RegisterFDServiceServer(grpcServer, &fdServer{})

	go func() {
		<-stopCh
		grpcServer.GracefulStop()
		close(doneCh)
	}()

	errCh <- grpcServer.Serve(lis)
}

func startClientAndSendFile(t *testing.T) {
	perRPCCredentials := grpcfd.PerRPCCredentials(nil)
	sender, ok := grpcfd.FromPerRPCCredentials(perRPCCredentials)
	if !ok {
		t.Fatal("No grpcfd sender is available")
	}

	conn, err := grpc.Dial("unix:"+socketPath,
		grpc.WithTransportCredentials(grpcfd.TransportCredentials(insecure.NewCredentials())),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		grpcfd.WithChainUnaryInterceptor(),
		grpcfd.WithChainStreamInterceptor(),
	)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewFDServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("Failed to open file %v", filePath)
	}
	defer file.Close()

	for i := 0; i < 3; i++ {
		file.WriteString("Written by the client...\n")
		errCh := sender.SendFD(file.Fd())

		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("Error occurred: %v", err)
			}
		default:
		}

		inodeURL, err := grpcfd.FilenameToURL(filePath)
		if err != nil {
			t.Fatalf("Error occurred: %v", err)
		}
		fd := &pb.FD{Name: inodeURL.String()}
		_, err = client.ServerReceive(ctx, fd, grpc.PerRPCCredentials(perRPCCredentials))
		if err != nil {
			t.Fatalf("Error occurred: %v", err)
		}
	}
}

func startClientAndRecvFile(t *testing.T) {
	perRPCCredentials := grpcfd.PerRPCCredentials(nil)
	recv, ok := grpcfd.FromPerRPCCredentials(perRPCCredentials)
	if !ok {
		t.Fatal("no grpcfd receiver is available")
	}

	conn, err := grpc.Dial("unix:"+socketPath,
		grpc.WithTransportCredentials(grpcfd.TransportCredentials(insecure.NewCredentials())),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		grpcfd.WithChainUnaryInterceptor(),
		grpcfd.WithChainStreamInterceptor(),
	)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewFDServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	for i := 0; i < 3; i++ {
		fd, err := client.ServerSend(ctx, &pb.FD{Name: ""}, grpc.PerRPCCredentials(perRPCCredentials))
		if err != nil {
			t.Fatalf("Error occurred: %v", err)
		}

		fileCh, err := recv.RecvFileByURL(fd.GetName())
		if err != nil {
			t.Fatalf("Error occurred: %v", err)
		}
		file := <-fileCh
		file.Seek(0, 0)

		_, err = io.ReadAll(file)
		if err != nil {
			t.Fatalf("Error occurred: %v", err)
		}
		defer file.Close()

		_, err = file.WriteString("Seen by client...\n")
		if err != nil {
			t.Fatalf("Error occurred: %v", err)
		}
	}
}

func TestServerRecvFileByURL(t *testing.T) {
	t.Log("Starting test")
	errCh := make(chan error)
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})

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

	startClientAndSendFile(t)
	t.Log("Client done")

	close(stopCh)
	<-doneCh

	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()
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
	t.Log("Cleanup complete")
}

func TestClientRecvFileByURL(t *testing.T) {
	t.Log("Starting test")
	errCh := make(chan error)
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})

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

	startClientAndRecvFile(t)
	t.Log("Client done")

	close(stopCh)
	<-doneCh

	file, err := os.Open(filePath2)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()
	readBuffer, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("Error while reading file: %v", err)
	}
	if string(readBuffer) != expectedContent2 {
		t.Fatalf("File content does not match. Got: \n%s", string(readBuffer))
	}

	if err := os.Remove(filePath2); err != nil {
		t.Fatalf("Failed to remove file: %v", err)
	}
	t.Log("Cleanup complete")
}
