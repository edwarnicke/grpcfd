//  Copyright (c) 2021 Cisco and/or its affiliates.
//
//  SPDX-License-Identifier: Apache-2.0
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at:
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package grpcfd

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type cobminedPerRPCCredentials []credentials.PerRPCCredentials

func (c cobminedPerRPCCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	var result = make(map[string]string)
	for _, creds := range c {
		m, err := creds.GetRequestMetadata(ctx, uri...)
		if err != nil {
			return nil, err
		}
		for k, v := range m {
			result[k] = v
		}
	}
	return result, nil
}

func (c cobminedPerRPCCredentials) RequireTransportSecurity() bool {
	for _, creds := range c {
		if creds.RequireTransportSecurity() {
			return true
		}
	}
	return false
}

func mergePerCredentialsCallOptions(callOpts ...grpc.CallOption) []grpc.CallOption {
	var result []grpc.CallOption
	var combinedCredentials cobminedPerRPCCredentials
	for _, opt := range callOpts {
		if v, ok := opt.(grpc.PerRPCCredsCallOption); ok {
			combinedCredentials = append(combinedCredentials, v.Creds)
		} else {
			result = append(result, opt)
		}
	}
	return append(result, grpc.PerRPCCredentials(combinedCredentials))
}

// WithChainUnaryInterceptor returns a DialOption that specifies the chained
// interceptor for unary RPCs. This interceptor combines all grpc.PerRPCCredsCallOption options into one.
// That allows using a few credentials.PerRPCCredentials passed from default call options and from the client call.
func WithChainUnaryInterceptor() grpc.DialOption {
	return grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(ctx, method, req, reply, cc, mergePerCredentialsCallOptions(opts...)...)
	})
}

// WithChainStreamInterceptor returns a DialOption that specifies the chained
// interceptor for streaming RPCs.This interceptor combines all grpc.PerRPCCredsCallOption options into one.
// That allows using a few credentials.PerRPCCredentials passed from default call options and from the client call.
func WithChainStreamInterceptor() grpc.DialOption {
	return grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(ctx, desc, cc, method, mergePerCredentialsCallOptions(opts...)...)
	})
}
