package streams

// State is a very simple key-value store.
type State[K comparable, V any] interface {
	Get(k K) (V, error)
	Put(k K, v V) error
	Each(func(K, V) error) error
}

type MapState[K comparable, V any] struct {
	m map[K]V
}

func (m *MapState[K, V]) Get(k K) (V, error) {
	v := m.m[k]
	return v, nil
}

func (m *MapState[K, V]) Put(k K, v V) error {
	m.m[k] = v
	return nil
}

func (m *MapState[K, V]) Each(f func(K, V) error) error {
	for k, v := range m.m {
		if err := f(k, v); err != nil {
			return err
		}
	}
	return nil
}

func NewMapState[K comparable, V any]() State[K, V] {
	return &MapState[K, V]{
		m: make(map[K]V),
	}
}
