package sokv

type Cursor[K any, V any] interface {
	Min() K
	Max() K
	Prev() (bool, error)
	Next() (bool, error)
	Seek(key K, isStart bool) error
	Key() K
	Value() V
}

type bpCursor[K any, V any] struct {
}

func (b *bpCursor[K, V]) Min() K {
	//TODO implement me
	panic("implement me")
}

func (b *bpCursor[K, V]) Max() K {
	//TODO implement me
	panic("implement me")
}

func (b *bpCursor[K, V]) Prev() (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (b *bpCursor[K, V]) Next() (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (b *bpCursor[K, V]) Seek(key K, isStart bool) error {
	//TODO implement me
	panic("implement me")
}

func (b *bpCursor[K, V]) Key() K {
	//TODO implement me
	panic("implement me")
}

func (b *bpCursor[K, V]) Value() V {
	//TODO implement me
	panic("implement me")
}
