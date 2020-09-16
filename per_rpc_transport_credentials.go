// Copyright (c) 2020 Cisco and/or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcfd

import (
	context "context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

type wrapPerRPCCredentials struct {
	credentials.PerRPCCredentials
	sendFunc func(sender FDSender)
}

// PerRPCCredentials - per rpc credentials that will, in addition to applying cred, invoke sendFunc
// Note: Must be used in concert with grpcfd.TransportCredentials
func PerRPCCredentials(cred credentials.PerRPCCredentials, sendFunc func(FDSender)) credentials.PerRPCCredentials {
	return &wrapPerRPCCredentials{
		PerRPCCredentials: cred,
		sendFunc:          sendFunc,
	}
}

// SendCallOptions - takes an optional list opts of grpc.CallOptions, and either prepends a
// grpc.PerRPCCredentials(PerRPCCredentials(nil, sendFunc)) or replaces an existing
// grpc.PerRPCCredsCallOption with one that has used PerRPCCredentials(..., sendFunc) to wrap
// the existing grpc.PerRPCCredsCallOption.
// Net-net: use this when you want a client to send files over a unix file socket using
// FDSender *before* the RPC is sent.
func SendCallOptions(sendFunc func(FDSender), opts ...grpc.CallOption) []grpc.CallOption {
	var rv []grpc.CallOption
	rv = append(rv, grpc.PerRPCCredentials(PerRPCCredentials(nil, sendFunc)))
	for _, opt := range opts {
		prcp, ok := opt.(grpc.PerRPCCredsCallOption)
		if ok {
			rv[0] = grpc.PerRPCCredentials(PerRPCCredentials(prcp.Creds, sendFunc))
			continue
		}
		rv = append(rv, opt)
	}
	return rv
}

func (w *wrapPerRPCCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	p, ok := peer.FromContext(ctx)
	if ok {
		sender, ok := p.Addr.(FDSender)
		if ok && w.sendFunc != nil {
			w.sendFunc(sender)
		}
	}
	if w.PerRPCCredentials != nil {
		return w.PerRPCCredentials.GetRequestMetadata(ctx, uri...)
	}
	return map[string]string{}, nil
}

func (w *wrapPerRPCCredentials) RequireTransportSecurity() bool {
	if w.PerRPCCredentials != nil {
		return w.PerRPCCredentials.RequireTransportSecurity()
	}
	return false
}
