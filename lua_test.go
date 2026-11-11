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

// A restore whose backup had no time left (PTTL 0) must leave the key absent,
// not persist it; -1 (no expiry) stays persistent and a positive PTTL is kept.
func TestRestoreValueOrDeleteScript_BackupPTTL(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	require.NoError(t, err)
	t.Cleanup(client.Close)

	for _, tc := range []struct {
		name      string
		pttl      string
		wantExist bool
		wantPTTL  func(int64) bool
	}{
		{"zero deletes", "0", false, nil},
		{"-2 deletes", "-2", false, nil},
		{"-1 stays persistent", "-1", true, func(p int64) bool { return p == -1 }},
		{"positive keeps ttl", "5000", true, func(p int64) bool { return p > 0 && p <= 5000 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := "restore-pttl:" + uuid.New().String()
			require.NoError(t, client.Do(ctx, client.B().Set().Key(key).Value("lock").Px(time.Minute).Build()).Error())
			n, err := restoreValueOrDeleteScript.Exec(ctx, client, []string{key}, []string{"lock", "1", "prior", tc.pttl}).AsInt64()
			require.NoError(t, err)
			require.EqualValues(t, 1, n)

			exists, err := client.Do(ctx, client.B().Exists().Key(key).Build()).AsInt64()
			require.NoError(t, err)
			require.Equal(t, tc.wantExist, exists == 1)
			if tc.wantExist {
				pttl, err := client.Do(ctx, client.B().Pttl().Key(key).Build()).AsInt64()
				require.NoError(t, err)
				require.True(t, tc.wantPTTL(pttl), "pttl=%d", pttl)
				t.Cleanup(func() { client.Do(ctx, client.B().Del().Key(key).Build()) })
			}
		})
	}
}
