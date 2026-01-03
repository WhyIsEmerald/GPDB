package generics

type Entry[V any] struct {
	Value       V
	IsTombstone bool
}
