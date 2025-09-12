package sokv

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBytesEqual(t *testing.T) {
	b := make([]byte, 32)
	require.True(t, bytesIsZero(b))
	b[16] = 1
	require.False(t, bytesIsZero(b))
}
