// Package mock adds helper classes to ease grpc server and client creation
package mock

import (
	"context"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	health "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/edwarnicke/grpcfd"
	"github.com/pkg/errors"
)

type sendfileCallbackType func(grpcfd.FDTransceiver) (<-chan error, string)

type Client struct {
	socketname        string
	perRPCCredentials credentials.PerRPCCredentials
	conn              *grpc.ClientConn
	sender            grpcfd.FDTransceiver
	cli               health.HealthClient
	sendfileMethod    sendfileCallbackType
}

func NewClient(socketname string, sendfile sendfileCallbackType) *Client {
	return &Client{
		socketname:        socketname,
		perRPCCredentials: grpcfd.PerRPCCredentials(nil),
		sendfileMethod:    sendfile,
	}
}

func (c *Client) Close() error {
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}

func (c *Client) Dial() error {
	if c.conn != nil {
		panic("conn was not nil!")
	}
	var opts []grpc.DialOption
	opts = append(opts,
		grpc.WithTransportCredentials(
			grpcfd.TransportCredentials(insecure.NewCredentials())),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(
			grpc.WaitForReady(true)),
	)

	conn, err := grpc.Dial("unix:"+c.socketname, opts...)
	c.conn = conn
	return err
}

func (c *Client) Send(ctx context.Context) error {
	if c.sendfileMethod == nil {
		return errors.New("Use send Dummy, or specify a callback")
	}
	sender, ok := grpcfd.FromPerRPCCredentials(c.perRPCCredentials)
	if !ok {
		return errors.New("Failed to create grpcfd.Transceiver from rpcCredentials!")
	}
	c.sender = sender
	errCh, filename := c.sendfileMethod(c.sender)

	select {
	case err := <-errCh:
		return err
	default:
	}

	f := health.HealthCheckRequest{Service: filename}

	c.cli = health.NewHealthClient(c.conn)
	_, err := c.cli.Check(ctx, &f, grpc.PerRPCCredentials(c.perRPCCredentials))
	return err
}

func (c *Client) Recv(ctx context.Context, filename string) (<-chan *os.File, error) {
	sender, ok := grpcfd.FromPerRPCCredentials(c.perRPCCredentials)
	if !ok {
		return nil, errors.New("Failed to create grpcfd.Transceiver from rpcCredentials!")
	}
	c.sender = sender

	c.cli = health.NewHealthClient(c.conn)
	hc := health.HealthCheckRequest{}
	_, err := c.cli.Check(ctx, &hc, grpc.PerRPCCredentials(c.perRPCCredentials))
	if err != nil {
		return nil, err
	}
	url, err := grpcfd.FilenameToURL(filename)
	if err != nil {
		return nil, err
	}
	fch, err := sender.RecvFileByURL(url.String())
	return fch, err
}
