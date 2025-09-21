package sokv

import (
	"encoding/binary"
	"encoding/json"
)

var (
	_ Codec[[]byte] = new(BytesCodec)
	_ Codec[string] = new(JsonTypeCodec[string])
)

type Codec[T any] interface {
	Unmarshal(data []byte, v *T) error
	Marshal(v *T) ([]byte, error)
}

type BytesCodec struct{}

func (b BytesCodec) Unmarshal(data []byte, v *[]byte) error {
	*v = data
	return nil
}

func (b BytesCodec) Marshal(v *[]byte) ([]byte, error) {
	return *v, nil
}

type Uint64Codec struct{}

func (u Uint64Codec) Unmarshal(data []byte, v *uint64) error {
	*v = binary.BigEndian.Uint64(data)
	return nil
}

func (u Uint64Codec) Marshal(v *uint64) (b []byte, err error) {
	b = binary.BigEndian.AppendUint64(b, *v)
	return
}

type JsonTypeCodec[T any] struct{}

func (j JsonTypeCodec[T]) Unmarshal(data []byte, v *T) error {
	return json.Unmarshal(data, v)
}

func (j JsonTypeCodec[T]) Marshal(v *T) ([]byte, error) {
	return json.Marshal(v)
}
