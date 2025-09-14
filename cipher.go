package sokv

import (
	"crypto/aes"
	"crypto/cipher"
	"sync"
)

// Cipher 加密不允许原地更新, 解密必须原地更新
type Cipher interface {
	Encrypt(plaintext []byte) (ciphertext []byte, err error)
	free(ciphertext []byte)
	Decrypt(ciphertext []byte) error
}

type aesCipher struct {
	pool   sync.Pool
	cipher cipher.Block
	isInit bool
}

func NewAseCipher(key []byte) (Cipher, error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return &aesCipher{
		cipher: c,
	}, err
}

func (a *aesCipher) init(size int) {
	if a.isInit {
		return
	}
	a.isInit = true
	a.pool = sync.Pool{
		New: func() interface{} {
			return make([]byte, size)
		},
	}
}

func (a *aesCipher) Encrypt(plaintext []byte) (ciphertext []byte, err error) {
	a.init(len(plaintext))
	ciphertext = a.pool.Get().([]byte)
	a.cipher.Encrypt(ciphertext, plaintext)
	return ciphertext, nil
}

func (a *aesCipher) free(ciphertext []byte) {
	a.pool.Put(ciphertext)
}

func (a *aesCipher) Decrypt(ciphertext []byte) error {
	a.init(len(ciphertext))
	a.cipher.Decrypt(ciphertext, ciphertext)
	return nil
}
