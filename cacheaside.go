package redcache

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/dcbickfo/redcache/internal/cmdx"
	"github.com/dcbickfo/redcache/internal/mapsx"
	"github.com/dcbickfo/redcache/internal/syncx"
	"github.com/google/uuid"
	"github.com/redis/rueidis"
)

type CacheAside struct {
	client  rueidis.Client
	locks   syncx.Map[string, chan struct{}]
	lockTTL time.Duration
}

type CacheAsideOption struct {
	LockTTL time.Duration
}

func NewRedCacheAside(clientOption rueidis.ClientOption, caOption CacheAsideOption) (*CacheAside, error) {
	if caOption.LockTTL == 0 {
		caOption.LockTTL = 10 * time.Second
	}

	rca := &CacheAside{
		lockTTL: caOption.LockTTL,
	}
	clientOption.OnInvalidations = rca.onInvalidate
	client, err := rueidis.NewClient(clientOption)
	if err != nil {
		return nil, err
	}
	rca.client = client
	return rca, nil
}

func (rca *CacheAside) Client() rueidis.Client {
	return rca.client
}

func (rca *CacheAside) onInvalidate(messages []rueidis.RedisMessage) {
	for _, m := range messages {
		key, _ := m.ToString()
		ch, loaded := rca.locks.LoadAndDelete(key)
		if loaded {
			close(ch)
		}
	}
}

const prefix = "redcache:"

var (
	delKeyLua = rueidis.NewLuaScript(`if redis.call("GET",KEYS[1]) == ARGV[1] then return redis.call("DEL",KEYS[1]) else return 0 end`)
	setKeyLua = rueidis.NewLuaScript(`if redis.call("GET",KEYS[1]) == ARGV[1] then return redis.call("SET",KEYS[1],ARGV[2],"PX",ARGV[3]) else return 0 end`)
)

func (rca *CacheAside) register(key string) <-chan struct{} {
	ch, _ := rca.locks.LoadOrStore(key, make(chan struct{}))
	return ch
}

func (rca *CacheAside) Get(
	ctx context.Context,
	ttl time.Duration,
	key string,
	fn func(ctx context.Context, key string) (val string, err error),
) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, ttl)
	defer cancel()
retry:
	wait := rca.register(key)
	val, err := rca.tryGet(ctx, ttl, key)

	if err != nil && !errors.Is(err, errNotFound) {
		return "", err
	}

	if err == nil && val != "" {
		return val, nil
	}

	if val == "" {
		val, err = rca.trySetKeyFunc(ctx, ttl, key, fn)
	}

	if err != nil && !errors.Is(err, errLockFailed) {
		return "", err
	}

	if val == "" {
		select {
		case <-wait:
			goto retry
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	return val, err
}

func (rca *CacheAside) Del(ctx context.Context, key string) error {
	return rca.client.Do(ctx, rca.client.B().Del().Key(key).Build()).Error()
}

func (rca *CacheAside) DelMulti(ctx context.Context, keys ...string) error {
	return rca.client.Do(ctx, rca.client.B().Del().Key(keys...).Build()).Error()
}

var errNotFound = errors.New("not found")
var errLockFailed = errors.New("lock failed")

func (rca *CacheAside) tryGet(ctx context.Context, ttl time.Duration, key string) (string, error) {
	resp := rca.client.DoCache(ctx, rca.client.B().Get().Key(key).Cache(), ttl)
	val, err := resp.ToString()
	if rueidis.IsRedisNil(err) || strings.HasPrefix(val, prefix) { // no response or is a lock value
		return "", errNotFound
	}
	if err != nil {
		return "", err
	}
	return val, nil
}

func (rca *CacheAside) trySetKeyFunc(ctx context.Context, ttl time.Duration, key string, fn func(ctx context.Context, key string) (string, error)) (val string, err error) {
	setVal := false
	lockVal, err := rca.tryLock(ctx, key)
	if err != nil {
		return "", err
	}
	defer func() {
		if !setVal {
			toCtx, cancel := context.WithTimeout(context.Background(), rca.lockTTL)
			defer cancel()
			rca.unlock(toCtx, key, lockVal)
		}
	}()
	if val, err = fn(ctx, key); err == nil {
		val, err = rca.setWithLock(ctx, ttl, key, valAndLock{val, lockVal})
		if err == nil {
			setVal = true
		}
		return val, err
	}
	return "", err
}

func (rca *CacheAside) tryLock(ctx context.Context, key string) (string, error) {
	uuidv7, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	lockVal := prefix + uuidv7.String()
	err = rca.client.Do(ctx, rca.client.B().Set().Key(key).Value(lockVal).Nx().Get().Px(rca.lockTTL).Build()).Error()
	if !rueidis.IsRedisNil(err) {
		return "", errLockFailed
	}
	return lockVal, nil
}

func (rca *CacheAside) setWithLock(ctx context.Context, ttl time.Duration, key string, valLock valAndLock) (string, error) {

	err := setKeyLua.Exec(ctx, rca.client, []string{key}, []string{valLock.lockVal, valLock.val, strconv.FormatInt(ttl.Milliseconds(), 10)}).Error()

	if err != nil {
		if !rueidis.IsRedisNil(err) {
			return "", err
		}
		return "", errors.New("set failed")
	}

	return valLock.val, nil
}

func (rca *CacheAside) unlock(ctx context.Context, key string, lock string) {
	delKeyLua.Exec(ctx, rca.client, []string{key}, []string{lock})
}

func (rca *CacheAside) GetMulti(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	fn func(ctx context.Context, key []string) (val map[string]string, err error),
) (map[string]string, error) {

	ctx, cancel := context.WithTimeout(ctx, ttl)
	defer cancel()

	res := make(map[string]string, len(keys))

	waitLock := make(map[string]<-chan struct{}, len(keys))
	for _, key := range keys {
		waitLock[key] = nil
	}

retry:
	waitLock = rca.registerAll(mapsx.Keys(waitLock))

	vals, err := rca.tryGetMulti(ctx, ttl, mapsx.Keys(waitLock))
	if err != nil && !rueidis.IsRedisNil(err) {
		return nil, err
	}

	for k, v := range vals {
		res[k] = v
		delete(waitLock, k)
	}

	if len(waitLock) > 0 {
		vals, err := rca.trySetMultiKeyFn(ctx, ttl, mapsx.Keys(waitLock), fn)
		if err != nil {
			return nil, err
		}
		for k, v := range vals {
			res[k] = v
			delete(waitLock, k)
		}
	}

	if len(waitLock) > 0 {
		err = syncx.WaitForAll(ctx, mapsx.Values(waitLock))
		if err != nil {
			return nil, err
		}
		goto retry
	}
	return res, err
}

func (rca *CacheAside) registerAll(keys []string) map[string]<-chan struct{} {
	res := make(map[string]<-chan struct{}, len(keys))
	for _, key := range keys {
		res[key] = rca.register(key)
	}
	return res
}

func (rca *CacheAside) tryGetMulti(ctx context.Context, ttl time.Duration, keys []string) (map[string]string, error) {
	multi := make([]rueidis.CacheableTTL, 0, len(keys))
	for _, key := range keys {
		cmd := rca.client.B().Get().Key(key).Cache()
		multi = append(multi, rueidis.CacheableTTL{
			Cmd: cmd,
			TTL: ttl,
		})
	}
	resps := rca.client.DoMultiCache(ctx, multi...)

	res := make(map[string]string)
	for i, resp := range resps {
		val, err := resp.ToString()
		if err != nil && rueidis.IsRedisNil(err) {
			continue
		} else if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(val, prefix) {
			res[keys[i]] = val
			continue
		}
	}
	return res, nil
}

func (rca *CacheAside) trySetMultiKeyFn(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	fn func(ctx context.Context, key []string) (val map[string]string, err error),
) (map[string]string, error) {

	res := make(map[string]string)

	lockVals, err := rca.tryLockMulti(ctx, keys)
	if err != nil {
		return nil, err
	}

	defer func() {
		toUnlock := make(map[string]string)
		for key, lockVal := range lockVals {
			if _, ok := res[key]; !ok {
				toUnlock[key] = lockVal
			}
		}
		if len(toUnlock) > 0 {
			toCtx, cancel := context.WithTimeout(context.Background(), rca.lockTTL)
			defer cancel()
			rca.unlockMulti(toCtx, toUnlock)
		}
	}()

	vals, err := fn(ctx, mapsx.Keys(lockVals))
	if err != nil {
		return nil, err
	}

	vL := make(map[string]valAndLock, len(vals))

	for k, v := range vals {
		vL[k] = valAndLock{v, lockVals[k]}
	}

	keysSet, err := rca.setMultiWithLock(ctx, ttl, vL)
	if err != nil {
		return nil, err
	}

	for _, keySet := range keysSet {
		res[keySet] = vals[keySet]
	}

	return res, err

}

func (rca *CacheAside) tryLockMulti(ctx context.Context, keys []string) (map[string]string, error) {
	lockVals := make(map[string]string, len(keys))
	cmds := make(rueidis.Commands, 0, len(keys))
	for _, k := range keys {
		uuidv7, err := uuid.NewV7()
		if err != nil {
			return nil, err
		}
		lockVals[k] = prefix + uuidv7.String()
		cmds = append(cmds, rca.client.B().Set().Key(k).Value(lockVals[k]).Nx().Get().Px(rca.lockTTL).Build())
	}
	resps := rca.client.DoMulti(ctx, cmds...)
	for i, r := range resps {
		err := r.Error()
		if !rueidis.IsRedisNil(err) {
			delete(lockVals, keys[i])
		}
	}
	return lockVals, nil
}

type valAndLock struct {
	val     string
	lockVal string
}

func (rca *CacheAside) setMultiWithLock(ctx context.Context, ttl time.Duration, keyValLock map[string]valAndLock) ([]string, error) {
	type keyOrderAndSet struct {
		keyOrder []string
		setStmts []rueidis.LuaExec
	}

	stmts := make(map[uint16]keyOrderAndSet)

	for k, vl := range keyValLock {
		slot := cmdx.Slot(k)
		kos, ok := stmts[slot]
		if !ok {
			kos = keyOrderAndSet{
				keyOrder: make([]string, 0),
				setStmts: make([]rueidis.LuaExec, 0),
			}
		}
		kos.keyOrder = append(kos.keyOrder, k)
		kos.setStmts = append(kos.setStmts, rueidis.LuaExec{
			Keys: []string{k},
			Args: []string{vl.lockVal, vl.val, strconv.FormatInt(ttl.Milliseconds(), 10)},
		})
		stmts[slot] = kos
	}

	out := make([]string, 0)
	for _, kos := range stmts {
		setResps := setKeyLua.ExecMulti(ctx, rca.client, kos.setStmts...)
		for i, resp := range setResps {
			err := resp.Error()
			if err != nil {
				if !rueidis.IsRedisNil(err) {
					return nil, err
				}
				continue
			}
			out = append(out, kos.keyOrder[i])
		}
	}
	return out, nil
}

func (rca *CacheAside) unlockMulti(ctx context.Context, lockVals map[string]string) {
	if len(lockVals) == 0 {
		return
	}
	delStmts := make([]rueidis.LuaExec, 0, len(lockVals))
	for key, lockVal := range lockVals {
		delStmts = append(delStmts, rueidis.LuaExec{
			Keys: []string{key},
			Args: []string{lockVal},
		})
	}
	delKeyLua.ExecMulti(ctx, rca.client, delStmts...)
}
