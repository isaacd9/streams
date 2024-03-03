package streams

import (
	"time"
)

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

type WindowState[K comparable, V any] interface {
	Put(WindowKey[K], V) error
	Get(WindowKey[K]) (V, error)
	Each(k K, start, end time.Time, fn func(WindowKey[K], V) error) error
	Every(start, end time.Time, fn func(WindowKey[K], V) error) error
}

type windowMapEntry[K comparable, V any] struct {
	Key   WindowKey[K]
	Value V
}

type MapWindowState[K comparable, V any] struct {
	m map[K][]windowMapEntry[K, V]
}

func NewMapWindowState[K comparable, V any]() WindowState[K, V] {
	return &MapWindowState[K, V]{
		m: make(map[K][]windowMapEntry[K, V]),
	}
}

func (m *MapWindowState[K, V]) Put(k WindowKey[K], v V) error {
	// TODO: Guarantee ordering
	m.m[k.K] = append(m.m[k.K], windowMapEntry[K, V]{
		Key:   k,
		Value: v,
	})
	return nil
}

func (m *MapWindowState[K, V]) Get(k WindowKey[K]) (V, error) {
	for _, e := range m.m[k.K] {
		if e.Key == k {
			return e.Value, nil
		}
	}
	var v V
	return v, nil
}

func (m *MapWindowState[K, V]) Each(k K, start, end time.Time, fn func(WindowKey[K], V) error) error {
	for _, e := range m.m[k] {
		if e.Key.Start.Before(start) || e.Key.End.After(end) {
			continue
		}
		if err := fn(e.Key, e.Value); err != nil {
			return err
		}
	}
	return nil
}

func (m *MapWindowState[K, V]) Every(start, end time.Time, fn func(WindowKey[K], V) error) error {
	for _, entries := range m.m {
		for _, e := range entries {
			if e.Key.Start.Before(start) || e.Key.End.After(end) {
				continue
			}
			if err := fn(e.Key, e.Value); err != nil {
				return err
			}
		}
	}
	return nil
}
