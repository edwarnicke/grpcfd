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
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/pkg/errors"
)

// FDToURL converts fd to URL of inode://${dev}/${ino}
func FDToURL(fd uintptr) (*url.URL, error) {
	var stat syscall.Stat_t
	err := syscall.Fstat(int(fd), &stat)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	u := &url.URL{
		Scheme: "inode",
		Host:   fmt.Sprintf("%d", stat.Dev),
		Path:   fmt.Sprintf("%d", stat.Ino),
	}
	return u, nil
}

// FileToURL converts file to URL of inode://${dev}/${ino}
func FileToURL(file SyscallConn) (u *url.URL, err error) {
	raw, err := file.SyscallConn()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	err = raw.Control(func(fd uintptr) {
		u, err = FDToURL(fd)
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return u, err
}

// FilenameToURL converts filename to URL of inode://${dev}/${ino}
func FilenameToURL(filename string) (u *url.URL, err error) {
	fi, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}
	u = &url.URL{
		Scheme: "inode",
		Host:   fmt.Sprintf("%d", fi.Sys().(*syscall.Stat_t).Dev),
		Path:   fmt.Sprintf("%d", fi.Sys().(*syscall.Stat_t).Ino),
	}
	return u, nil
}

// URLToDevIno - converts url of form inode://${dev}/${ino} url to dev,ino
func URLToDevIno(u *url.URL) (dev, ino uint64, err error) {
	if u.Scheme != "inode" {
		return 0, 0, errors.Errorf("scheme must be \"inode\" not %q", u.Scheme)
	}
	dev, err = strconv.ParseUint(u.Host, 10, 64)
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}
	ino, err = strconv.ParseUint(strings.TrimPrefix(u.Path, "/"), 10, 64)
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}
	return dev, ino, nil
}

// URLStringToDevIno converts url of form inode://${dev}/${ino} to dev,ino
func URLStringToDevIno(urlstr string) (dev, ino uint64, err error) {
	u, err := url.Parse(urlstr)
	if err != nil {
		return 0, 0, err
	}
	return URLToDevIno(u)
}
