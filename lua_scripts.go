package redcache

import "github.com/redis/rueidis"

// Lua scripts for CacheAside lock operations.
var (
	// delKeyLua atomically deletes a key only if the current value matches the lock.
	delKeyLua = rueidis.NewLuaScript(`if redis.call("GET",KEYS[1]) == ARGV[1] then return redis.call("DEL",KEYS[1]) else return 0 end`)
	// setKeyLua atomically sets a value only if the current value matches the lock (CAS).
	// Returns 1 on success, 0 if the lock was lost. The explicit `return 1` matters:
	// Redis's SET reply is the status string "OK", which AsInt64 cannot parse — letting
	// it bubble up as the script's return would make every successful CAS look like
	// a lock-lost (bogus LockLost metric + retry).
	setKeyLua = rueidis.NewLuaScript(`if redis.call("GET",KEYS[1]) == ARGV[1] then redis.call("SET",KEYS[1],ARGV[2],"PX",ARGV[3]) return 1 else return 0 end`)
)

// Lua scripts for PrimeableCacheAside write-lock operations.
var (
	// acquireWriteLockWithBackupScript atomically acquires a write lock and
	// returns the previous value plus its PTTL for rollback. Unlike SET NX
	// (used by Get), this allows overwriting real values but refuses to
	// overwrite an existing lock, preventing Set from stomping on an active
	// Get operation's lock.
	//
	// Returns: [success (0 or 1), previous_value or false, previous_pttl].
	// previous_pttl is -1 (persistent), positive (ms remaining), or 0 when
	// previous_value is false.
	acquireWriteLockWithBackupScript = rueidis.NewLuaScript(`
		local key = KEYS[1]
		local lock_value = ARGV[1]
		local ttl = ARGV[2]
		local lock_prefix = ARGV[3]

		local current = redis.call("GET", key)

		if current == false then
			redis.call("SET", key, lock_value, "PX", ttl)
			return {1, false, 0}
		end

		if string.sub(current, 1, string.len(lock_prefix)) == lock_prefix then
			return {0, current, 0}
		end

		local pttl = redis.call("PTTL", key)
		redis.call("SET", key, lock_value, "PX", ttl)
		return {1, current, pttl}
	`)

	// restoreValueOrDeleteScript CAS-restores a saved value or deletes the key.
	// Used during Set/SetMulti rollback. Only acts if we still hold our lock.
	//
	// ARGV: lock, had_saved ("0" or "1"), restore_value, restore_pttl.
	// had_saved distinguishes "no prior value" (DEL) from "prior value was
	// empty string" (SET with empty). restore_pttl preserves the original TTL:
	// "0" or negative means persistent (SET without PX); positive means
	// SET PX <ms>.
	restoreValueOrDeleteScript = rueidis.NewLuaScript(`
		local key = KEYS[1]
		local expected_lock = ARGV[1]
		local had_saved = ARGV[2]
		local restore_value = ARGV[3]
		local restore_pttl = tonumber(ARGV[4]) or 0

		if redis.call("GET", key) ~= expected_lock then
			return 0
		end
		if had_saved == "1" then
			if restore_pttl > 0 then
				redis.call("SET", key, restore_value, "PX", restore_pttl)
			else
				redis.call("SET", key, restore_value)
			end
		else
			redis.call("DEL", key)
		end
		return 1
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

	// touchScript extends a key's TTL via PEXPIRE only if the current value is a
	// real (non-lock) value. No-ops if the key is missing or holds a lock value
	// to avoid extending an in-flight lock by accident.
	// Returns 1 on PEXPIRE applied, 0 if skipped.
	touchScript = rueidis.NewLuaScript(`
		local cur = redis.call("GET", KEYS[1])
		if cur == false then
			return 0
		end
		local lock_prefix = ARGV[2]
		if string.sub(cur, 1, string.len(lock_prefix)) == lock_prefix then
			return 0
		end
		return redis.call("PEXPIRE", KEYS[1], ARGV[1])
	`)

	// refreshAheadSetScript writes a refreshed value only if the current value is
	// a real (non-lock) value. Skips if the key is missing (let normal Get-on-miss
	// handle population) or holds a lock value (a Get/Set is already in progress
	// and will write its own current value). Returns 1 on write, 0 if skipped.
	refreshAheadSetScript = rueidis.NewLuaScript(`
		local cur = redis.call("GET", KEYS[1])
		if cur == false then
			return 0
		end
		local lock_prefix = ARGV[3]
		if string.sub(cur, 1, string.len(lock_prefix)) == lock_prefix then
			return 0
		end
		redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
		return 1
	`)
)
