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
	"fmt"
	"net"
	"os"
	"runtime"
	"syscall"

	"github.com/edwarnicke/serialize"
	"github.com/pkg/errors"
	"google.golang.org/grpc/peer"
)

const (
	maxFDCount = 64
)

// SyscallConn - having the SyscallConn method to access syscall.RawConn
type SyscallConn interface {
	SyscallConn() (syscall.RawConn, error)
}

// FDSender - capable of Sending a file
type FDSender interface {
	SendFD(fd uintptr) <-chan error
	SendFile(file SyscallConn) <-chan error
	SendFilename(filename string) <-chan error
}

// FDRecver - capable of Recving an fd by (dev,ino)
type FDRecver interface {
	RecvFD(dev, inode uint64) <-chan uintptr
	RecvFile(dev, ino uint64) <-chan *os.File
	RecvFileByURL(urlStr string) (<-chan *os.File, error)
	RecvFDByURL(urlStr string) (<-chan uintptr, error)
}

// FDTransceiver - combination of FDSender  and FDRecver
type FDTransceiver interface {
	FDSender
	FDRecver
}

type inodeKey struct {
	dev uint64
	ino uint64
}

type connWrap struct {
	net.Conn

	sendFDs      []int
	errChs       []chan error
	sendExecutor serialize.Executor

	recvFDChans  map[inodeKey][]chan uintptr
	recvedFDs    map[inodeKey]uintptr
	recvExecutor serialize.Executor
}

func wrapConn(conn net.Conn) net.Conn {
	if _, ok := conn.(*connWrap); ok {
		return conn
	}
	_, ok := conn.(interface {
		WriteMsgUnix(b, oob []byte, addr *net.UnixAddr) (n, oobn int, err error)
	})
	if !ok {
		return conn
	}
	conn = &connWrap{
		Conn:        conn,
		recvFDChans: make(map[inodeKey][]chan uintptr),
		recvedFDs:   make(map[inodeKey]uintptr),
	}
	runtime.SetFinalizer(conn, func(conn *connWrap) {
		_ = conn.close()
	})
	return conn
}

func (w *connWrap) Close() error {
	runtime.SetFinalizer(w, nil)
	return w.close()
}

func (w *connWrap) close() error {
	err := w.Conn.Close()
	w.recvExecutor.AsyncExec(func() {
		for k, fd := range w.recvedFDs {
			_ = syscall.Close(int(fd))
			delete(w.recvedFDs, k)
		}
		for k, recvChs := range w.recvFDChans {
			for _, recvCh := range recvChs {
				close(recvCh)
			}
			delete(w.recvFDChans, k)
		}
		w.sendExecutor.AsyncExec(func() {
			for k, fd := range w.sendFDs {
				w.errChs[k] <- errors.Errorf("unable to send fd %d because connection closed", fd)
				close(w.errChs[k])
				_ = syscall.Close(fd)
			}
			w.sendFDs = nil
			w.errChs = nil
		})
	})
	return err
}

func (w *connWrap) Write(b []byte) (int, error) {
	var n int
	var err error
	<-w.sendExecutor.AsyncExec(func() {
		var sendFDs []int
		var errChs []chan error
		if len(w.sendFDs) > 0 {
			limit := len(w.sendFDs)
			if maxFDCount < limit {
				limit = maxFDCount
			}
			sendFDs = w.sendFDs[:limit]
			w.sendFDs = w.sendFDs[limit:]
			errChs = w.errChs[:limit]
			w.errChs = w.errChs[limit:]
		}

		for i, fd := range sendFDs {
			rights := syscall.UnixRights(fd)
			// TODO handle when n != 1 and handle oobn not as expected
			// maybe with a for {n == 0} ?
			// maybe if oobn == 0 we simply prepend the remainder of the fds to w.sendFDs and call it good?
			_, _, err = w.Conn.(interface {
				WriteMsgUnix(b, oob []byte, addr *net.UnixAddr) (n, oobn int, err error)
			}).WriteMsgUnix([]byte{b[i]}, rights, nil)
			if err != nil {
				errChs[i] <- err
			}
			close(errChs[i])
			_ = syscall.Close(fd)
		}
		n, err = w.Conn.Write(b[len(sendFDs):])
		if err == nil {
			n += len(sendFDs)
		}
	})
	return n, err
}

func (w *connWrap) SendFD(fd uintptr) <-chan error {
	errCh := make(chan error, 1)
	// Dup the fd because we have no way of knowing what the caller will do with it between
	// now and when we can send it
	fd, _, err := syscall.Syscall(syscall.SYS_FCNTL, fd, uintptr(syscall.F_DUPFD), 0)
	if err != 0 {
		errCh <- errors.WithStack(err)
		close(errCh)
		return errCh
	}
	w.sendExecutor.AsyncExec(func() {
		w.sendFDs = append(w.sendFDs, int(fd))
		w.errChs = append(w.errChs, errCh)
	})
	return errCh
}

func (w *connWrap) SendFile(file SyscallConn) <-chan error {
	errCh := make(chan error, 1)
	raw, err := file.SyscallConn()
	if err != nil {
		errCh <- errors.Wrapf(err, "unable to retrieve syscall.RawConn for src %+v", file)
		close(errCh)
		return errCh
	}
	err = raw.Control(func(fd uintptr) {
		var stat syscall.Stat_t
		statErr := syscall.Fstat(int(fd), &stat)
		if statErr != nil {
			errCh <- statErr
			close(errCh)
			return
		}
		go func(errChIn <-chan error, errChOut chan<- error) {
			for err := range errChIn {
				errChOut <- err
			}
			close(errChOut)
		}(w.SendFD(fd), errCh)
	})
	if err != nil {
		errCh <- err
		close(errCh)
	}
	return errCh
}

func (w *connWrap) RemoteAddr() net.Addr {
	return w
}

func (w *connWrap) Network() string {
	return w.Conn.RemoteAddr().Network()
}

func (w *connWrap) String() string {
	return w.Conn.RemoteAddr().String()
}

func (w *connWrap) RecvFD(dev, ino uint64) <-chan uintptr {
	fdCh := make(chan uintptr, 1)
	w.recvExecutor.AsyncExec(func() {
		key := inodeKey{
			dev: dev,
			ino: ino,
		}
		// If we have the fd for this (dev,ino) already
		if fd, ok := w.recvedFDs[key]; ok {
			// Copy it
			var errno syscall.Errno
			fd, _, errno = syscall.Syscall(syscall.SYS_FCNTL, fd, uintptr(syscall.F_DUPFD), 0)
			if errno != 0 {
				// TODO - this is terrible error handling
				close(fdCh)
				return
			}
			// Send it to the requestor
			fdCh <- fd
			// Close the channel
			close(fdCh)
			return
		}
		// Otherwise queue the requestor up to receive the fd if we ever get it
		w.recvFDChans[key] = append(w.recvFDChans[key], fdCh)
	})
	return fdCh
}

func (w *connWrap) RecvFDByURL(urlStr string) (<-chan uintptr, error) {
	dev, ino, err := URLStringToDevIno(urlStr)
	if err != nil {
		return nil, err
	}
	return w.RecvFD(dev, ino), nil
}

func (w *connWrap) RecvFile(dev, ino uint64) <-chan *os.File {
	fileCh := make(chan *os.File, 1)
	go func(fdCh <-chan uintptr, fileCh chan<- *os.File) {
		for fd := range fdCh {
			if runtime.GOOS == "linux" {
				fileCh <- os.NewFile(fd, fmt.Sprintf("/proc/%d/fd/%d", os.Getpid(), fd))
				continue
			}
			fileCh <- os.NewFile(fd, "")
		}
		close(fileCh)
	}(w.RecvFD(dev, ino), fileCh)
	return fileCh
}

func (w *connWrap) RecvFileByURL(urlStr string) (<-chan *os.File, error) {
	dev, ino, err := URLStringToDevIno(urlStr)
	if err != nil {
		return nil, err
	}
	return w.RecvFile(dev, ino), nil
}

func (w *connWrap) Read(b []byte) (n int, err error) {
	oob := make([]byte, syscall.CmsgSpace(4*maxFDCount))
	n, oobn, _, _, err := w.Conn.(interface {
		ReadMsgUnix(b, oob []byte) (n, oobn, flags int, addr *net.UnixAddr, err error)
	}).ReadMsgUnix(b, oob)

	// Go async for updating info
	w.recvExecutor.AsyncExec(func() {
		// We got oob info
		if oobn != 0 {
			msgs, parseCtlErr := syscall.ParseSocketControlMessage(oob[:oobn])
			if parseCtlErr != nil {
				return
			}
			for i := range msgs {
				fds, parseRightsErr := syscall.ParseUnixRights(&msgs[i])
				if parseRightsErr != nil {
					return
				}
				for _, fd := range fds {
					var stat syscall.Stat_t

					// Get the (dev,ino) for the fd
					fstatErr := syscall.Fstat(fd, &stat)
					if fstatErr != nil {
						continue
					}
					key := inodeKey{
						dev: uint64(stat.Dev),
						ino: stat.Ino,
					}

					// If we have one already... close the new one
					if _, ok := w.recvedFDs[key]; ok {
						_ = syscall.Close(fd)
						continue
					}

					// If its new store it in our map of recvedFDs
					w.recvedFDs[key] = uintptr(fd)

					// Iterate through any waiting receivers
					for _, fdCh := range w.recvFDChans[key] {
						// Copy the fd.  Always copy the fd.  Who knows what the recipient might choose to do with it.
						fd, _, errno := syscall.Syscall(syscall.SYS_FCNTL, w.recvedFDs[key], uintptr(syscall.F_DUPFD), 0)
						if errno != 0 { // TODO - this is terrible error handling
							close(fdCh)
							continue
						}
						fdCh <- fd
						close(fdCh)
					}
					delete(w.recvFDChans, key)
				}
			}
		}
	})
	if err != nil {
		return 0, err
	}
	return n, err
}

// FromPeer - return grpcfd.FDTransceiver from peer.Peer
//            ok is true of successful, false otherwise
func FromPeer(p *peer.Peer) (transceiver FDTransceiver, ok bool) {
	transceiver, ok = p.Addr.(FDTransceiver)
	return transceiver, ok
}

// FromContext - return grpcfd.FDTransceiver from context.Context
//               ok is true of successful, false otherwise
func FromContext(ctx context.Context) (transceiver FDTransceiver, ok bool) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, false
	}
	return FromPeer(p)
}
