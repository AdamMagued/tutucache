//go:build windows

package geecache

import (
	"os"

	"golang.org/x/sys/windows"
)

// fileLock 是一把跨进程排他锁（Windows 使用 LockFileEx）。
// 进程退出时系统会释放文件锁，不会留下死锁。
type fileLock struct {
	f  *os.File
	ov *windows.Overlapped
}

func acquireFileLock(path string) (*fileLock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	ov := new(windows.Overlapped)
	err = windows.LockFileEx(
		windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK,
		0, 1, 0, ov,
	)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &fileLock{f: f, ov: ov}, nil
}

func (l *fileLock) release() {
	_ = windows.UnlockFileEx(windows.Handle(l.f.Fd()), 0, 1, 0, l.ov)
	_ = l.f.Close()
}
