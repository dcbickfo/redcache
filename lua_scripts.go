package redcache

import "github.com/redis/rueidis"

// Lua scripts for CacheAside lock operations.
var (
	// delKeyLua atomically deletes a key only if the current value matches the lock.
	delKeyLua = rueidis.NewLuaScript(`if redis.call("GET",KEYS[1]) == ARGV[1] then return redis.call("DEL",KEYS[1]) else return 0 end`)
	// setKeyLua atomically sets a value only if the current value matches the lock (CAS).
	setKeyLua = rueidis.NewLuaScript(`if redis.call("GET",KEYS[1]) == ARGV[1] then return redis.call("SET",KEYS[1],ARGV[2],"PX",ARGV[3]) else return 0 end`)
)

// Lua scripts for PrimeableCacheAside write-lock operations.
var (
	// acquireWriteLockScript atomically acquires a write lock.
	// Unlike SET NX (used by Get), this allows overwriting real values but
	// refuses to overwrite an existing lock, preventing Set from stomping
	// on an active Get operation's lock.
	// Returns 1 on success, 0 if an existing lock is present.
	acquireWriteLockScript = rueidis.NewLuaScript(`
		local key = KEYS[1]
		local lock_value = ARGV[1]
		local ttl = ARGV[2]
		local lock_prefix = ARGV[3]

		local current = redis.call("GET", key)

		if current == false then
			redis.call("SET", key, lock_value, "PX", ttl)
			return 1
		end

		if string.sub(current, 1, string.len(lock_prefix)) == lock_prefix then
			return 0
		end

		redis.call("SET", key, lock_value, "PX", ttl)
		return 1
	`)

	// acquireWriteLockWithBackupScript acquires a lock and returns the previous value
	// for rollback in SetMulti.
	// Returns: [success (0 or 1), previous_value or false].
	acquireWriteLockWithBackupScript = rueidis.NewLuaScript(`
		local key = KEYS[1]
		local lock_value = ARGV[1]
		local ttl = ARGV[2]
		local lock_prefix = ARGV[3]

		local current = redis.call("GET", key)

		if current == false then
			redis.call("SET", key, lock_value, "PX", ttl)
			return {1, false}
		end

		if string.sub(current, 1, string.len(lock_prefix)) == lock_prefix then
			return {0, current}
		end

		redis.call("SET", key, lock_value, "PX", ttl)
		return {1, current}
	`)

	// restoreValueOrDeleteScript CAS-restores a saved value or deletes the key.
	// Used during SetMulti rollback. Only acts if we still hold our lock.
	restoreValueOrDeleteScript = rueidis.NewLuaScript(`
		local key = KEYS[1]
		local expected_lock = ARGV[1]
		local restore_value = ARGV[2]

		if redis.call("GET", key) == expected_lock then
			if restore_value and restore_value ~= "" then
				redis.call("SET", key, restore_value)
			else
				redis.call("DEL", key)
			end
			return 1
		else
			return 0
		end
	`)

	// setWithWriteLockScript is a strict CAS: SET value only if we hold the exact lock.
	// Returns 1 on success, 0 if lock was lost.
	setWithWriteLockScript = rueidis.NewLuaScript(`
		local key = KEYS[1]
		local value = ARGV[1]
		local ttl = ARGV[2]
		local expected_lock = ARGV[3]

		if redis.call("GET", key) == expected_lock then
			redis.call("SET", key, value, "PX", ttl)
			return 1
		else
			return 0
		end
	`)

	// refreshLockScript CAS-refreshes a lock's TTL using PEXPIRE.
	// Unlike re-SET, PEXPIRE does not trigger Redis invalidation messages
	// and cannot overwrite a value written by another operation between
	// lock acquisition and TTL refresh.
	// Returns 1 on success, 0 if lock was lost.
	refreshLockScript = rueidis.NewLuaScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			redis.call("PEXPIRE", KEYS[1], ARGV[2])
			return 1
		else
			return 0
		end
	`)
)
