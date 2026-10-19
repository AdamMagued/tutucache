//go:build !windows && !darwin && !dragonfly && !freebsd && !illumos && !linux && !netbsd && !openbsd

package geecache

import (
	"errors"
	"fmt"
	"os"
	"time"
)

// 该平台既没有 flock 也没有 LockFileEx，用 O_EXCL 锁文件实现跨进程互斥。
// Register 的临界区只有几毫秒；超过 staleLockAge 的锁视为进程崩溃残留并回收。
const (
	lockPollInterval = 5 * time.Millisecond
	lockWaitTimeout  = 10 * time.Second
	staleLockAge     = 5 * time.Second
)

type fileLock struct {
	path  string
	token string // 写入锁文件，释放时据此判断锁是否已被别人回收
}

func acquireFileLock(path string) (*fileLock, error) {
	deadline := time.Now().Add(lockWaitTimeout)
	token := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	for {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err == nil {
			_, werr := f.WriteString(token)
			cerr := f.Close()
			if werr != nil || cerr != nil {
				_ = os.Remove(path)
				return nil, fmt.Errorf("write lock %s: %w", path, errors.Join(werr, cerr))
			}
			return &fileLock{path: path, token: token}, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}

		if info, statErr := os.Stat(path); statErr == nil &&
			time.Since(info.ModTime()) > staleLockAge {
			_ = os.Remove(path)
			continue
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("lock %s: timeout", path)
		}
		time.Sleep(lockPollInterval)
	}
}

func (l *fileLock) release() {
	// 只删除自己持有的锁；如果锁因超时被其它进程回收，不要把别人的锁删掉。
	if data, err := os.ReadFile(l.path); err == nil && string(data) == l.token {
		_ = os.Remove(l.path)
	}
}
