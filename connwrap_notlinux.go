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

// +build !linux,!windows

package grpcfd

import (
	"os"
)

func (w *connWrap) SendFilename(filename string) <-chan error {
	errCh := make(chan error, 1)
	// Note: this will fail in most cases for 'unopenable' files (like unix file sockets).  See use of O_PATH in connwrap_linux.go for
	// the trick that makes this work in Linux
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
	_ = file.Close()
	return errCh
}
