//go:build !linux

package hostcheck

const unsupported = "仅在 Linux 上可探测"

func probeNewMountAPI() (bool, string) { return false, unsupported }
func probeCloseRange() (bool, string)  { return false, unsupported }
func probeUserNS() (bool, string)      { return false, unsupported }
func probeUserNSMax() (bool, string)   { return false, unsupported }

func probeSeccompActions() (bool, string)  { return false, unsupported }
func probeCloneIntoCgroup() (bool, string) { return false, unsupported }
func probePidfd() (bool, string)           { return false, unsupported }
