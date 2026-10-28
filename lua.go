package redcache

import (
	"context"
	"crypto/sha1" //nolint:gosec // SHA-1 is Redis's script-cache key, not a security boundary.
	"encoding/hex"

	"github.com/redis/rueidis"
)

// luaScript is a rueidis.Lua whose ExecMulti relies on Redis's script cache
// instead of priming it. rueidis's ExecMulti sends SCRIPT LOAD to every node,
// replicas included, on every call; this one pipelines EVALSHA and re-sends
// only the NOSCRIPT replies as EVAL, which also caches the script on that node.
// A NOSCRIPT reply means the script never ran, so the re-send is safe. Exec is
// rueidis's, which already falls back the same way.
type luaScript struct {
	*rueidis.Lua
	src  string
	sha1 string
}

func newLuaScript(src string) *luaScript {
	sum := sha1.Sum([]byte(src)) //nolint:gosec // See import.
	return &luaScript{Lua: rueidis.NewLuaScript(src), src: src, sha1: hex.EncodeToString(sum[:])}
}

// ExecMulti runs the script once per exec in one pipeline. Results are in exec
// order.
func (s *luaScript) ExecMulti(ctx context.Context, c rueidis.Client, multi ...rueidis.LuaExec) []rueidis.RedisResult {
	cmdsP := commandsPool.Get(len(multi))
	defer commandsPool.Put(cmdsP)
	cmds := *cmdsP
	for i, m := range multi {
		cmds[i] = c.B().Evalsha().Sha1(s.sha1).Numkeys(int64(len(m.Keys))).Key(m.Keys...).Arg(m.Args...).Build()
	}
	resps := c.DoMulti(ctx, cmds...)

	var noScript []int
	for i, resp := range resps {
		if err, ok := rueidis.IsRedisErr(resp.Error()); ok && err.IsNoScript() {
			noScript = append(noScript, i)
		}
	}
	if len(noScript) == 0 {
		return resps
	}
	cmds = cmds[:0]
	for _, i := range noScript {
		m := multi[i]
		cmds = append(cmds, c.B().Eval().Script(s.src).Numkeys(int64(len(m.Keys))).Key(m.Keys...).Arg(m.Args...).Build())
	}
	for j, resp := range c.DoMulti(ctx, cmds...) {
		resps[noScript[j]] = resp
	}
	return resps
}
