package errs_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dcbickfo/redcache/internal/errs"
)

func TestErrors(t *testing.T) {
	t.Run("ErrLockFailed has correct message", func(t *testing.T) {
		assert.Equal(t, "lock acquisition failed", errs.ErrLockFailed.Error())
	})

	t.Run("ErrLockLost has correct message", func(t *testing.T) {
		assert.Equal(t, "lock was lost or expired before value could be set", errs.ErrLockLost.Error())
	})

	t.Run("ErrLockFailed can be wrapped and unwrapped", func(t *testing.T) {
		wrappedErr := errors.Join(errs.ErrLockFailed, errors.New("additional context"))
		assert.ErrorIs(t, wrappedErr, errs.ErrLockFailed)
	})

	t.Run("ErrLockLost can be wrapped and unwrapped", func(t *testing.T) {
		wrappedErr := errors.Join(errs.ErrLockLost, errors.New("additional context"))
		assert.ErrorIs(t, wrappedErr, errs.ErrLockLost)
	})

	t.Run("errors are distinct", func(t *testing.T) {
		assert.NotEqual(t, errs.ErrLockFailed, errs.ErrLockLost)
		assert.False(t, errors.Is(errs.ErrLockFailed, errs.ErrLockLost))
		assert.False(t, errors.Is(errs.ErrLockLost, errs.ErrLockFailed))
	})
}
