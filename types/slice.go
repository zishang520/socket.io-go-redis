package types

type Slice[F any, T any] []F

func (s Slice[F, T]) Map(f func(value F) T) []T {
	r := make([]T, len(s))
	for i, v := range s {
		r[i] = f(v)
	}
	return r
}

func (s Slice[F, T]) Range(f func(key int, value F) bool) {
	for k, v := range s {
		if !f(k, v) {
			break
		}
	}
}
