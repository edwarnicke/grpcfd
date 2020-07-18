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
	"fmt"
	"net"
	"os"
	"runtime"
	"syscall"

	"github.com/edwarnicke/serialize"
	"github.com/pkg/errors"
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
}

type inodeKey struct {
	dev uint64
	ino uint64
}

type wrappedConn struct {
	net.Conn

	sendFD       []func(b []byte) (n int, err error)
	sendExecutor serialize.Executor

	recvFDChans  map[inodeKey][]chan uintptr
	unclaimedFD  map[inodeKey][]uintptr
	recvExecutor serialize.Executor
}

func wrapConn(conn net.Conn) net.Conn {
	_, ok := conn.(interface {
		WriteMsgUnix(b, oob []byte, addr *net.UnixAddr) (n, oobn int, err error)
	})
	if !ok {
		return conn
	}
	conn = &wrappedConn{
		Conn: conn,
	}
	runtime.SetFinalizer(conn, func(conn net.Conn) {
		_ = conn.Close()
	})
	return conn
}

func (w *wrappedConn) Close() error {
	err := w.Conn.Close()
	w.recvExecutor.AsyncExec(func() {
		for k, fds := range w.unclaimedFD {
			for _, fd := range fds {
				_ = syscall.Close(int(fd))
			}
			delete(w.unclaimedFD, k)
		}
		for k, recvChs := range w.recvFDChans {
			for _, recvCh := range recvChs {
				close(recvCh)
			}
			delete(w.recvFDChans, k)
		}
	})
	return err
}

func (w *wrappedConn) Write(b []byte) (int, error) {
	var write func(b []byte) (int, error)
	<-w.sendExecutor.AsyncExec(func() {
		if len(w.sendFD) > 0 {
			write = w.sendFD[0]
		}
	})
	if write != nil {
		n, err := write(b)
		return n, err
	}
	n, err := w.Conn.Write(b)
	if err != nil {
		return 0, err
	}
	return n, err
}

func (w *wrappedConn) SendFD(fd uintptr) <-chan error {
	errCh := make(chan error, 1)
	w.sendExecutor.AsyncExec(func() {
		w.sendFD = append(w.sendFD, func(b []byte) (n int, err error) {
			rights := syscall.UnixRights(int(fd))
			n, _, err = w.Conn.(interface {
				WriteMsgUnix(b, oob []byte, addr *net.UnixAddr) (n, oobn int, err error)
			}).WriteMsgUnix(b, rights, nil)
			w.sendFD = w.sendFD[1:]
			errCh <- err
			close(errCh)
			return n, err
		})
	})
	return errCh
}

func (w *wrappedConn) SendFile(file SyscallConn) <-chan error {
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

func (w *wrappedConn) SendFilename(filename string) <-chan error {
	errCh := make(chan error, 1)
	file, err := os.Open(filename) // #nosec
	if err != nil {
		errCh <- err
		close(errCh)
		return errCh
	}
	go func(errChIn <-chan error, errChOut chan<- error) {
		for err := range errChIn {
			errChOut <- err
		}
		err := file.Close()
		if err != nil {
			errChOut <- err
		}
		close(errChOut)
	}(w.SendFile(file), errCh)
	return errCh
}

func (w *wrappedConn) RemoteAddr() net.Addr {
	return w
}

func (w *wrappedConn) Network() string {
	return w.Conn.RemoteAddr().Network()
}

func (w *wrappedConn) String() string {
	return w.Conn.RemoteAddr().String()
}

func (w *wrappedConn) RecvFD(dev, ino uint64) <-chan uintptr {
	fdCh := make(chan uintptr)
	w.recvExecutor.AsyncExec(func() {
		key := inodeKey{
			dev: dev,
			ino: ino,
		}
		if len(w.unclaimedFD[key]) > 0 {
			fdCh <- w.unclaimedFD[key][0]
			w.unclaimedFD[key] = w.unclaimedFD[key][1:]
			if len(w.unclaimedFD[key]) == 0 {
				delete(w.unclaimedFD, key)
			}
			return
		}
		w.recvFDChans[key] = append(w.recvFDChans[key], fdCh)
	})
	return fdCh
}

func (w *wrappedConn) RecvFile(dev, ino uint64) <-chan *os.File {
	fileCh := make(chan *os.File)
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

func (w *wrappedConn) Read(b []byte) (n int, err error) {
	oob := make([]byte, syscall.CmsgSpace(4))
	var oobn int
	n, oobn, _, _, err = w.Conn.(interface {
		ReadMsgUnix(b, oob []byte) (n, oobn, flags int, addr *net.UnixAddr, err error)
	}).ReadMsgUnix(b, oob)
	w.recvExecutor.AsyncExec(func() {
		if oobn != 0 {
			msgs, parseCtlErr := syscall.ParseSocketControlMessage(oob)
			if parseCtlErr != nil {
				return
			}
			fds, parseRightsErr := syscall.ParseUnixRights(&msgs[0])
			if parseRightsErr != nil {
				return
			}
			for _, fd := range fds {
				var stat syscall.Stat_t
				err = syscall.Fstat(fd, &stat)
				if err != nil {
					continue
				}
				key := inodeKey{
					dev: uint64(stat.Dev),
					ino: stat.Ino,
				}
				for _, fdCh := range w.recvFDChans[key] {
					fdCh <- uintptr(fd)
					return
				}
				w.unclaimedFD[key] = append(w.unclaimedFD[key], uintptr(fd))
			}
		}
	})
	if err != nil {
		return 0, err
	}
	return n, err
}
