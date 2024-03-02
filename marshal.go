package streams

import "fmt"

func UnmarshalString(r Reader[[]byte, []byte]) Reader[string, string] {
	return Map(r, func(r Message) Record[string, string] {
		return Record[string, string]{
			Key:   string(r.Key),
			Value: string(r.Value),
		}
	})
}

func MarshalAny[K, V any](r Reader[K, V]) Reader[[]byte, []byte] {
	return Map(r, func(r Record[K, V]) Message {
		return Message{
			Key:   []byte(fmt.Sprintf("%v", r.Key)),
			Value: []byte(fmt.Sprintf("%v", r.Value)),
		}
	})
}
