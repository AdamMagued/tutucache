//go:build darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd

package geecache

import (
	"os"
	"syscall"
)

// fileLock 是一把跨进程排他锁。
// flock 锁在进程退出（fd 关闭）时由内核自动释放，不会留下死锁。
type fileLock struct {
	f *os.File
}

func acquireFileLock(path string) (*fileLock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return &fileLock{f: f}, nil
}

func (l *fileLock) release() {
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	_ = l.f.Close()
}
