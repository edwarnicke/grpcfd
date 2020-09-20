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
	context "context"
	"net"

	"google.golang.org/grpc/credentials"
)

type wrapTransportCredentials struct {
	capturers []func(FDSender)
	credentials.TransportCredentials
}

// TransportCredentials - transport credentials that will, in addition to applying cred, cause peer.Addr to supply
// the FDSender and FDRecver interfaces
func TransportCredentials(cred credentials.TransportCredentials, capturers ...func(FDSender)) credentials.TransportCredentials {
	if _, ok := cred.(*wrapTransportCredentials); ok {
		return cred
	}
	return &wrapTransportCredentials{
		TransportCredentials: cred,
		capturers:            capturers,
	}
}

func (c *wrapTransportCredentials) ClientHandshake(ctx context.Context, authority string, rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn := wrapConn(rawConn)
	fdsender, ok := conn.(FDSender)
	var authInfo credentials.AuthInfo
	var err error
	if c.TransportCredentials != nil {
		conn, authInfo, err = c.TransportCredentials.ClientHandshake(ctx, authority, conn)
	}
	if ok {
		for _, capturer := range c.capturers {
			capturer(fdsender)
		}
	}
	return conn, authInfo, err
}

func (c *wrapTransportCredentials) ServerHandshake(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn := wrapConn(rawConn)
	var authInfo credentials.AuthInfo
	var err error
	if c.TransportCredentials != nil {
		conn, authInfo, err = c.TransportCredentials.ServerHandshake(conn)
	}
	if fdsender, ok := conn.(FDSender); ok {
		for _, presender := range c.capturers {
			presender(fdsender)
		}
	}
	return conn, authInfo, err
}

func (c *wrapTransportCredentials) Clone() credentials.TransportCredentials {
	if c.TransportCredentials != nil {
		return &wrapTransportCredentials{
			TransportCredentials: c.TransportCredentials.Clone(),
			capturers:            c.capturers,
		}
	}
	return &wrapTransportCredentials{capturers: c.capturers}
}

func (c *wrapTransportCredentials) Info() credentials.ProtocolInfo {
	if c.TransportCredentials == nil {
		return credentials.ProtocolInfo{}
	}
	return c.TransportCredentials.Info()
}

func (c *wrapTransportCredentials) OverrideServerName(s string) error {
	if c.TransportCredentials == nil {
		return nil
	}
	return c.TransportCredentials.OverrideServerName(s)
}
