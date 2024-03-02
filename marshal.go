package streams

import "fmt"

func UnmarshalString(r Reader[[]byte, []byte]) Reader[string, string] {
	return Map(r, func(r KeyValue[[]byte, []byte]) KeyValue[string, string] {
		return KeyValue[string, string]{
			Key:   string(r.Key),
			Value: string(r.Value),
		}
	})
}

func MarshalAny[K, V any](r Reader[K, V]) Reader[[]byte, []byte] {
	return Map(r, func(r KeyValue[K, V]) KeyValue[[]byte, []byte] {
		return KeyValue[[]byte, []byte]{
			Key:   []byte(fmt.Sprintf("%v", r.Key)),
			Value: []byte(fmt.Sprintf("%v", r.Value)),
		}
	})
}
