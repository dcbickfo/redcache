package syncx

import (
	"context"
	"iter"
	"reflect"
)

func WaitForAll[C ~<-chan V, V any](ctx context.Context, waitLock iter.Seq[C], length int) error {
	cases := setupCases(ctx, waitLock, length)
	for range length {
		chosen, _, _ := reflect.Select(cases)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		cases = append(cases[:chosen], cases[chosen+1:]...)
	}
	return nil
}

func setupCases[C ~<-chan V, V any](ctx context.Context, waitLock iter.Seq[C], length int) []reflect.SelectCase {
	cases := make([]reflect.SelectCase, length+1)
	i := 0
	for ch := range waitLock {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
		}
		i++
	}
	cases[i] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	}
	return cases
}
