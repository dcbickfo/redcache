package syncx

import (
	"context"
	"reflect"
)

func WaitForAll[S ~[]C, C ~<-chan V, V any](ctx context.Context, waitLock S) error {
	cases := setupCases(ctx, waitLock)
	for range waitLock {
		chosen, _, _ := reflect.Select(cases)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		cases = append(cases[:chosen], cases[chosen+1:]...)
	}
	return nil
}

func setupCases[S ~[]C, C ~<-chan V, V any](ctx context.Context, waitLock S) []reflect.SelectCase {
	cases := make([]reflect.SelectCase, len(waitLock)+1)
	i := 0
	for _, ch := range waitLock {
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
