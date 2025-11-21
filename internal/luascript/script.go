// Package luascript provides a common interface for Lua script execution in Redis.
package luascript

import (
	"context"

	"github.com/redis/rueidis"
)

// Executor defines the interface for executing Lua scripts in Redis.
// This abstraction allows for consistent script handling across the codebase.
type Executor interface {
	// Exec executes the Lua script with the given keys and arguments.
	Exec(ctx context.Context, client rueidis.Client, keys, args []string) rueidis.RedisResult

	// ExecMulti executes multiple instances of the script with different parameters.
	ExecMulti(ctx context.Context, client rueidis.Client, statements ...rueidis.LuaExec) []rueidis.RedisResult
}

// New creates a new Lua script executor that wraps rueidis.Lua.
// This provides a consistent interface for all Lua script operations.
func New(script string) Executor {
	return &executor{
		script: rueidis.NewLuaScript(script),
	}
}

// executor wraps rueidis.Lua to implement the Executor interface.
type executor struct {
	script *rueidis.Lua
}

// Exec implements Executor.
func (e *executor) Exec(ctx context.Context, client rueidis.Client, keys, args []string) rueidis.RedisResult {
	return e.script.Exec(ctx, client, keys, args)
}

// ExecMulti implements Executor.
func (e *executor) ExecMulti(ctx context.Context, client rueidis.Client, statements ...rueidis.LuaExec) []rueidis.RedisResult {
	return e.script.ExecMulti(ctx, client, statements...)
}
