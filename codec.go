package sokv

import (
	"encoding/json"
	"fmt"
)

var (
	_ Codec[string] = new(BaseTypeCodec[string])
	_ Codec[string] = new(JsonTypeCodec[string])
)

type Codec[T any] interface {
	Unmarshal(data []byte, v *T) error
	Marshal(v *T) ([]byte, error)
}

type BaseTypeCodec[T any] struct{}

func (b BaseTypeCodec[T]) Unmarshal(data []byte, v *T) error {
	switch any(v).(type) {
	case *int:
		break
	case *int8:
		break
	case *int16:
		break
	case *int32:
		break
	case *int64:
		break
	case *uint:
		break
	case *uint8:
		break
	case *uint16:
		break
	case *uint32:
		break
	case *uint64:
		break
	case *float32:
		break
	case *float64:
		break
	case *string:
		break
	case *[]byte:
		break
	default:
		return fmt.Errorf("unknown type: %T", any(v))
	}
}

func (b BaseTypeCodec[T]) Marshal(v *T) ([]byte, error) {
	switch any(v).(type) {
	case *int:
		break
	case *int8:
		break
	case *int16:
		break
	case *int32:
		break
	case *int64:
		break
	case *uint:
		break
	case *uint8:
		break
	case *uint16:
		break
	case *uint32:
		break
	case *uint64:
		break
	case *float32:
		break
	case *float64:
		break
	case *string:
		break
	case *[]byte:
		break
	default:
		return nil, fmt.Errorf("unknown type: %T", any(v))
	}
}

type JsonTypeCodec[T any] struct{}

func (j JsonTypeCodec[T]) Unmarshal(data []byte, v *T) error {
	return json.Unmarshal(data, v)
}

func (j JsonTypeCodec[T]) Marshal(v *T) ([]byte, error) {
	return json.Marshal(v)
}
