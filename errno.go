package sokv

import "errors"

var (
	errPageIdOverflow  = errors.New("page id overflow")
	errNoAvailablePage = errors.New("no available page")
)
