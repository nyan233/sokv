package sokv

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMMapPageStorage(t *testing.T) {
	initTest(t)
	s := newMMapPageStorage("testdata/test.db")
	require.NoError(t, s.init())
	res, err := s.allocPage(16)
	require.NoError(t, err)
	mRes, err := json.Marshal(map[string]any{
		"id":        3453945,
		"msg":       "my is test message",
		"tag":       "my tag",
		"user-name": "my user name",
	})
	require.NoError(t, err)
	for _, v := range res {
		pd, err := s.readPage(v)
		require.NoError(t, err)
		copy(pd.Data, mRes)
	}
	require.NoError(t, s.close())
	require.NoError(t, s.init())
	for _, v := range res {
		pd, err := s.readPage(v)
		require.NoError(t, err)
		require.Equal(t, string(pd.Data[:len(mRes)]), string(mRes))
	}
}
