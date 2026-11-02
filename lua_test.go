package redcache

import (
	"context"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"
	"uuid"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

var scriptLoadCalls = regexp.MustCompile(`cmdstat_script\|load:calls=(\d+)`)

func TestLuaScript_ExecMultiFallsBackToEvalWithoutScriptLoad(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	require.NoError(t, err)
	t.Cleanup(client.Close)

	loads := func() int {
		info, err := client.Do(ctx, client.B().Info().Section("commandstats").Build()).ToString()
		require.NoError(t, err)
		m := scriptLoadCalls.FindStringSubmatch(info)
		if m == nil {
			return 0
		}
		n, err := strconv.Atoi(m[1])
		require.NoError(t, err)
		return n
	}

	keys := []string{"lua:" + uuid.New().String(), "lua:" + uuid.New().String()}
	execs := make([]rueidis.LuaExec, len(keys))
	for i, k := range keys {
		require.NoError(t, client.Do(ctx, client.B().Set().Key(k).Value("v").Build()).Error())
		execs[i] = rueidis.LuaExec{Keys: []string{k}, Args: []string{strconv.FormatInt(time.Minute.Milliseconds(), 10), "__redcache:lock:"}}
	}
	// An empty script cache forces the NOSCRIPT → EVAL path on the first call;
	// the second call then hits EVALSHA.
	require.NoError(t, client.Do(ctx, client.B().ScriptFlush().Build()).Error())
	before := loads()

	for range 2 {
		for i, resp := range touchScript.ExecMulti(ctx, client, execs...) {
			n, err := resp.AsInt64()
			require.NoError(t, err, keys[i])
			require.Equal(t, int64(1), n, keys[i])
		}
	}
	require.Equal(t, before, loads(), "ExecMulti must not send SCRIPT LOAD")
}

// BenchmarkLuaScript_ExecMulti compares rueidis's Lua.ExecMulti, which sends
// SCRIPT LOAD to every node and waits for it before each batch, with
// luaScript.ExecMulti. The cluster cases run when REDCACHE_CLUSTER_ADDRS is set.
func BenchmarkLuaScript_ExecMulti(b *testing.B) {
	targets := []struct{ name, addr string }{{"standalone", "127.0.0.1:6379"}}
	if addr := os.Getenv("REDCACHE_CLUSTER_ADDRS"); addr != "" {
		targets = append(targets, struct{ name, addr string }{"cluster", addr})
	}
	ctx := context.Background()
	for _, target := range targets {
		client, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{target.addr}})
		require.NoError(b, err)
		b.Cleanup(client.Close)
		for _, n := range []int{1, 16, 64} {
			execs := make([]rueidis.LuaExec, n)
			prefix := "luabench:" + uuid.New().String() + ":"
			for i := range execs {
				// The keys never exist, so delKeyLua only does a GET.
				execs[i] = rueidis.LuaExec{Keys: []string{prefix + strconv.Itoa(i)}, Args: []string{"lock"}}
			}
			for _, impl := range []struct {
				name string
				exec func(context.Context, rueidis.Client, ...rueidis.LuaExec) []rueidis.RedisResult
			}{
				{"rueidis", delKeyLua.Lua.ExecMulti},
				{"redcache", delKeyLua.ExecMulti},
			} {
				b.Run(target.name+"/keys="+strconv.Itoa(n)+"/"+impl.name, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						for _, r := range impl.exec(ctx, client, execs...) {
							if err := r.Error(); err != nil {
								b.Fatal(err)
							}
						}
					}
				})
			}
		}
	}
}
