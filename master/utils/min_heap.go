package utils

import (
	"errors"
	"reflect"
)

// An MinHeap is a min-heap of ints.
type MinHeap []int32

func (h MinHeap) Len() int           { return len(h) }
func (h MinHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h MinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

// Push min heap push function
func (h *MinHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(int32))
}

// Pop min heap pop function
func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// PopInt pops an integer value from the heap
func (h *MinHeap) PopInt() (int32, error) {
	intType := reflect.TypeOf(int32(0))

	value := h.Pop()
	v := reflect.ValueOf(value)
	if !v.Type().ConvertibleTo(intType) {
		// The read value is not an integer, return error
		return -1, errors.New("PopInt: Trying to pop non integer value")
	}
	fv := v.Convert(intType)
	return int32(fv.Int()), nil
}
