//go:build !linux

package hostcheck

const unsupported = "仅在 Linux 上可探测"

func probeNewMountAPI() (bool, string) { return false, unsupported }
func probeCloseRange() (bool, string)  { return false, unsupported }
func probeUserNS() (bool, string)      { return false, unsupported }
