// Package openfdcount counts ho many open references are to a file
//
// it uses 'fuser' to get which pids use the specified filename, and then greps through /proc/<pid>/fdinfo/, to get how
// many times the process has opened the file.
//
// caviats: It will not show file references if after open the file has been deleted, but has not yet been closed.
package openfdcount

import (
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"syscall"

	"github.com/edwarnicke/grpcfd"
	"github.com/pkg/errors"
)

func emptyByteSilce() []byte {
	return []byte{}
}

func FilenameToInode(filename string) (uint64, error) {
	fileinfo, err := os.Stat(filename)
	if err != nil {
		return 0, err
	}
	stat := fileinfo.Sys()
	inode := stat.(*syscall.Stat_t).Ino
	return inode, nil
}

func FilenameToDevIno(filename string) (dev, ino uint64, err error) {
	u, err := grpcfd.FilenameToURL(filename)
	if err != nil {
		return 0, 0, err
	}
	return grpcfd.URLToDevIno(u)
}

func fuser(filename string) ([]string, error) {
	cmd := exec.Command("/usr/bin/fuser", filename)
	res, err := cmd.CombinedOutput()

	if err != nil && len(res) == 0 {
		return nil, nil
	} else if err != nil {
		return nil, errors.New(string(res))
	}

	s := string(res)
	s = strings.Trim(s, " \n")
	slc := strings.Split(s, ":")
	s = strings.Trim(slc[1], " \n")
	slc = strings.Split(s, " ")
	slc = slices.DeleteFunc(slc, func(elem string) bool {
		return elem == ""
	})

	return slc, nil
}

func OpenFDCount(filename string) (int, error) {
	slc, err := fuser(filename)
	if err != nil {
		return 0, err
	}
	_, ino, err := FilenameToDevIno(filename)
	if err != nil {
		return 0, err
	}
	ocount := 0
	for _, pid := range slc {
		fdinfo := "/proc/" + pid + "/fdinfo"
		entries, err := os.ReadDir(fdinfo)
		if err != nil {
			return 0, err
		}
		for _, entry := range entries {
			entryPath := fdinfo + "/" + entry.Name()
			_, err := os.Stat(entryPath)
			if err != nil {
				continue
			}
			// #nosec: 204 I couldn't find any other workaround to this, and inside fdinfo there shouldn't be anything  malicious
			res, err := exec.Command("grep", "-e", "^ino:\t"+fmt.Sprint(ino), entryPath).CombinedOutput()
			switch e := err.(type) {
			case *exec.ExitError:
				switch ec := e.ProcessState.ExitCode(); ec {
				case 0: // exit success, exec.Command should not return error, handle it at the default branch in the outer switch
				case 1: // there was no match in grep, do not increment count
				default:
					return 0, errors.Errorf("grep returned with error: %v, exit_code: %d stoud: %v", err, ec, string(res))
				}
			case error:
				return 0, errors.Errorf("grep returned with error: %v", err)
			default:
				ocount++
			}
		}
	}
	return ocount, nil
}
