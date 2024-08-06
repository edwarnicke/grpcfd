package mock

import (
	"context"

	"github.com/edwarnicke/grpcfd"
	"github.com/pkg/errors"

	health "google.golang.org/grpc/health/grpc_health_v1"
)

//---------------------------------------------------------------------------//

type callbackType = func(grpcfd.FDTransceiver, string) (*health.HealthCheckResponse, error)

type MockServer struct {
	health.UnimplementedHealthServer
	callback callbackType
}

func NewMockServer(cb callbackType) *MockServer {
	return &MockServer{
		callback: cb,
	}
}
func (s *MockServer) Check(ctx context.Context, hc *health.HealthCheckRequest) (*health.HealthCheckResponse, error) {
	recv, ok := grpcfd.FromContext(ctx)
	if !ok {
		err := errors.New("Couldn't get FDReceiver from context!")
		return nil, err
	}
	return s.callback(recv, hc.Service)
}
