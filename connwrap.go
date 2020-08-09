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
	RecvFileByURL(urlStr string) (<-chan *os.File, error)
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
	recvedFDs    map[inodeKey]uintptr
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
		Conn:        conn,
		recvFDChans: make(map[inodeKey][]chan uintptr),
		recvedFDs:   make(map[inodeKey]uintptr),
	}
	runtime.SetFinalizer(conn, func(conn net.Conn) {
		_ = conn.Close()
	})
	return conn
}

func (w *wrappedConn) Close() error {
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

func (w *wrappedConn) RecvFDByURL(urlStr string) (<-chan uintptr, error) {
	dev, ino, err := URLStringToDevIno(urlStr)
	if err != nil {
		return nil, err
	}
	return w.RecvFD(dev, ino), nil
}

func (w *wrappedConn) RecvFile(dev, ino uint64) <-chan *os.File {
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

func (w *wrappedConn) RecvFileByURL(urlStr string) (<-chan *os.File, error) {
	dev, ino, err := URLStringToDevIno(urlStr)
	if err != nil {
		return nil, err
	}
	return w.RecvFile(dev, ino), nil
}

func (w *wrappedConn) Read(b []byte) (n int, err error) {
	oob := make([]byte, syscall.CmsgSpace(4))
	var oobn int
	n, oobn, _, _, err = w.Conn.(interface {
		ReadMsgUnix(b, oob []byte) (n, oobn, flags int, addr *net.UnixAddr, err error)
	}).ReadMsgUnix(b, oob)

	// Go async for updating info
	w.recvExecutor.AsyncExec(func() {
		// We got oob info
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
					if errno != 0 {
						// TODO - this is terrible error handling
						close(fdCh)
						continue
					}
					fdCh <- fd
					close(fdCh)
				}
				delete(w.recvFDChans, key)
			}
		}
	})
	if err != nil {
		return 0, err
	}
	return n, err
}
