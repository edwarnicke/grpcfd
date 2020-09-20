//  Copyright (c) 2020 Cisco and/or its affiliates.
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

// +build !windows

package grpcfd

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type credentialsDialOption struct {
	creds     credentials.TransportCredentials
	capturers []func(FDSender)
	grpc.DialOption
}

// WithTransportCredentials - drop in replacement for grpc.WithTransportCredentials, that will, in addition to applying cred, cause peer.Addr to supply
// the FDSender and FDRecver interfaces.
func WithTransportCredentials(creds credentials.TransportCredentials, capturers ...func(FDSender)) grpc.DialOption {
	return &credentialsDialOption{
		creds:      creds,
		capturers:  capturers,
		DialOption: grpc.WithTransportCredentials(TransportCredentials(creds)),
	}
}

func (c *credentialsDialOption) addSenderCapture(presenders ...func(FDSender)) grpc.DialOption {
	return WithTransportCredentials(c.creds.Clone(), append(c.capturers, presenders...)...)
}

// CaptureSender - given a list of dialopts ... if one of them is a grpcfd.TransportCredentials option... add another
// capturer to capture FDSenders on dial
func CaptureSender(capturer func(FDSender), dialopts ...grpc.DialOption) (opts []grpc.DialOption, ok bool) {
	var rvDialOpts []grpc.DialOption
	var rvOK bool
	for _, dialopt := range dialopts {
		if cdo, ok := dialopt.(interface {
			AddPresenders(presenders ...func(FDSender)) grpc.DialOption
		}); ok {
			rvOK = true
			rvDialOpts = append(rvDialOpts, cdo.AddPresenders(capturer))
			continue
		}
		rvDialOpts = append(rvDialOpts, dialopt)
	}
	return rvDialOpts, rvOK
}
