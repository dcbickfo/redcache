// Package logger provides a common logging interface for all redcache components.
package logger

// Logger defines the logging interface used throughout redcache.
// Implementations must be safe for concurrent use and should handle log levels internally.
type Logger interface {
	// Error logs error messages. Should be used for unexpected failures or critical issues.
	Error(msg string, args ...any)

	// Debug logs detailed diagnostic information useful for development and troubleshooting.
	// Call Debug to record verbose output about internal state, cache operations, or lock handling.
	// Debug messages should not include sensitive information and may be omitted in production.
	Debug(msg string, args ...any)
}
