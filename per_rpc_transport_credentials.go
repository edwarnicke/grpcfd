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

// +build !windows

package grpcfd

import (
	"context"

	"github.com/edwarnicke/serialize"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type wrapPerRPCCredentials struct {
	credentials.PerRPCCredentials
	senderFuncs []func(FDSender)
	executor    serialize.Executor
}

func (w *wrapPerRPCCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	<-w.executor.AsyncExec(func() {
		if sender, ok := FromContext(ctx); ok {
			for _, f := range w.senderFuncs {
				f(sender)
			}
		}
	})
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

func (w *wrapPerRPCCredentials) SendFD(fd uintptr) <-chan error {
	out := make(chan error, 1)
	w.executor.AsyncExec(func() {
		w.senderFuncs = append(w.senderFuncs, func(sender FDSender) {
			go joinErrChs(sender.SendFD(fd), out)
		})
	})
	return out
}

func (w *wrapPerRPCCredentials) SendFile(file SyscallConn) <-chan error {
	out := make(chan error, 1)
	w.executor.AsyncExec(func() {
		w.senderFuncs = append(w.senderFuncs, func(transceiver FDSender) {
			go joinErrChs(transceiver.SendFile(file), out)
		})
	})
	return out
}

func joinErrChs(in <-chan error, out chan<- error) {
	for err := range in {
		out <- err
	}
	close(out)
}

// PerRPCCredentials - per rpc credentials that will, in addition to applying cred, invoke sendFunc
// Note: Must be used in concert with grpcfd.TransportCredentials
func PerRPCCredentials(cred credentials.PerRPCCredentials) credentials.PerRPCCredentials {
	if _, ok := cred.(*wrapPerRPCCredentials); ok {
		return cred
	}
	return &wrapPerRPCCredentials{
		PerRPCCredentials: cred,
	}
}

// PerRPCCredentialsFromCallOptions - extract credentials.PerRPCCredentials from a list of grpc.CallOptions
func PerRPCCredentialsFromCallOptions(opts ...grpc.CallOption) credentials.PerRPCCredentials {
	for i := len(opts) - 1; i >= 0; i-- {
		if prcp, ok := opts[i].(grpc.PerRPCCredsCallOption); ok {
			return prcp.Creds
		}
	}
	return nil
}

// FromPerRPCCredentials - return grpcfd.FDTransceiver from credentials.PerRPCCredentials
//                         ok is true of successful, false otherwise
func FromPerRPCCredentials(rpcCredentials credentials.PerRPCCredentials) (sender FDSender, ok bool) {
	if sender, ok = rpcCredentials.(FDSender); ok {
		return sender, true
	}
	return nil, false
}
