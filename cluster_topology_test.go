package redcache_test

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

// These tests change the cluster's topology, so none of them is parallel: Go
// runs every sequential top-level test before resuming the parallel ones, so
// the other cluster tests never see a half-moved slot or a failover. Each test
// restores the original layout in t.Cleanup.

// clusterNode is one line of CLUSTER NODES.
type clusterNode struct {
	id, addr, primaryID string
	primary             bool
}

// adminClient opens a plain, uncached connection to one cluster node for
// commands that must reach that node rather than be routed by slot.
func adminClient(t *testing.T, addr string) rueidis.Client {
	t.Helper()
	c, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:       []string{addr},
		ForceSingleClient: true,
		DisableCache:      true,
	})
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

func do(t *testing.T, c rueidis.Client, args ...string) rueidis.RedisResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c.Do(ctx, c.B().Arbitrary(args...).Build())
}

// topologyTest opens a cluster client, fails fast unless the cluster is
// healthy with three primaries, and bounds the test's Redis calls so a broken
// topology fails the test instead of hanging it.
func topologyTest(t *testing.T) (context.Context, *redcache.Cache, *redcache.Conn, []clusterNode) {
	t.Helper()
	client, conn := makeClusterClient(t)
	info, err := do(t, adminClient(t, "127.0.0.1:30001"), "CLUSTER", "INFO").ToString()
	require.NoError(t, err)
	require.Contains(t, info, "cluster_state:ok", "cluster is unhealthy; reset it with `docker compose up -d --force-recreate --wait redis-cluster`")
	nodes := clusterNodes(t, "127.0.0.1:30001")
	var primaries int
	for _, n := range nodes {
		if n.primary {
			primaries++
		}
	}
	require.Equal(t, 3, primaries, "cluster must have three primaries; reset it with `docker compose up -d --force-recreate --wait redis-cluster`")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx, client, conn, nodes
}

// clusterNodes reads the topology as seen by node addr.
func clusterNodes(t *testing.T, addr string) []clusterNode {
	t.Helper()
	out, err := do(t, adminClient(t, addr), "CLUSTER", "NODES").ToString()
	require.NoError(t, err)
	// RESP3 sends CLUSTER NODES as a verbatim string, which keeps its "txt:"
	// format prefix.
	out = strings.TrimPrefix(out, "txt:")
	var nodes []clusterNode
	for line := range strings.Lines(out) {
		f := strings.Fields(line)
		if len(f) < 8 || strings.Contains(f[2], "fail") {
			continue
		}
		n := clusterNode{id: f[0], addr: strings.Split(f[1], "@")[0], primaryID: f[3]}
		n.primary = strings.Contains(f[2], "master")
		nodes = append(nodes, n)
	}
	return nodes
}

func keySlot(t *testing.T, conn *redcache.Conn, key string) int {
	t.Helper()
	c := conn.Client()
	slot, err := c.Do(t.Context(), c.B().ClusterKeyslot().Key(key).Build()).AsInt64()
	require.NoError(t, err)
	return int(slot)
}

// slotOwner returns the primary serving slot, by asking CLUSTER SLOTS.
func slotOwner(t *testing.T, nodes []clusterNode, slot int) clusterNode {
	t.Helper()
	slots, err := do(t, adminClient(t, nodes[0].addr), "CLUSTER", "SLOTS").ToArray()
	require.NoError(t, err)
	for _, s := range slots {
		r, err := s.ToArray()
		require.NoError(t, err)
		lo, _ := r[0].AsInt64()
		hi, _ := r[1].AsInt64()
		if int64(slot) < lo || int64(slot) > hi {
			continue
		}
		owner, err := r[2].ToArray()
		require.NoError(t, err)
		id, err := owner[2].ToString()
		require.NoError(t, err)
		for _, n := range nodes {
			if n.id == id {
				return n
			}
		}
	}
	t.Fatalf("slot %d has no owner", slot)
	return clusterNode{}
}

func otherPrimary(t *testing.T, nodes []clusterNode, not clusterNode) clusterNode {
	t.Helper()
	for _, n := range nodes {
		if n.primary && n.id != not.id {
			return n
		}
	}
	t.Fatal("no second primary")
	return clusterNode{}
}

func port(addr string) string { return addr[strings.LastIndex(addr, ":")+1:] }

// moveSlotKeys MIGRATEs every key in slot from one node to another.
func moveSlotKeys(t *testing.T, slot int, from, to clusterNode) {
	t.Helper()
	src := adminClient(t, from.addr)
	for {
		keys, err := do(t, src, "CLUSTER", "GETKEYSINSLOT", strconv.Itoa(slot), "100").AsStrSlice()
		require.NoError(t, err)
		if len(keys) == 0 {
			return
		}
		// Nodes share the container, so 127.0.0.1 reaches the destination.
		args := append([]string{"MIGRATE", "127.0.0.1", port(to.addr), "", "0", "5000", "KEYS"}, keys...)
		require.NoError(t, do(t, src, args...).Error())
	}
}

// beginSlotMigration marks slot as IMPORTING on to and MIGRATING on from.
func beginSlotMigration(t *testing.T, slot int, from, to clusterNode) {
	t.Helper()
	s := strconv.Itoa(slot)
	require.NoError(t, do(t, adminClient(t, to.addr), "CLUSTER", "SETSLOT", s, "IMPORTING", from.id).Error())
	require.NoError(t, do(t, adminClient(t, from.addr), "CLUSTER", "SETSLOT", s, "MIGRATING", to.id).Error())
}

// finishSlotMigration moves the remaining keys and assigns slot to on every
// primary.
func finishSlotMigration(t *testing.T, nodes []clusterNode, slot int, from, to clusterNode) {
	t.Helper()
	moveSlotKeys(t, slot, from, to)
	for _, n := range nodes {
		if n.primary {
			require.NoError(t, do(t, adminClient(t, n.addr), "CLUSTER", "SETSLOT", strconv.Itoa(slot), "NODE", to.id).Error())
		}
	}
}

func migrateSlot(t *testing.T, nodes []clusterNode, slot int, from, to clusterNode) {
	t.Helper()
	beginSlotMigration(t, slot, from, to)
	finishSlotMigration(t, nodes, slot, from, to)
}

func flushScripts(t *testing.T, addr string) {
	t.Helper()
	require.NoError(t, do(t, adminClient(t, addr), "SCRIPT", "FLUSH").Error())
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func role(t *testing.T, addr string) string {
	t.Helper()
	r, err := do(t, adminClient(t, addr), "ROLE").ToArray()
	if err != nil {
		return ""
	}
	s, _ := r[0].ToString()
	return s
}

func taggedKeys(tag string, n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("topo:{%s}:%d", tag, i)
	}
	return keys
}

func exactLoader(want map[string]string) func(context.Context, []string) (map[string]string, error) {
	return func(_ context.Context, ks []string) (map[string]string, error) {
		out := make(map[string]string, len(ks))
		for _, k := range ks {
			out[k] = want[k]
		}
		return out, nil
	}
}

func noLoader(context.Context, []string) (map[string]string, error) {
	return nil, errors.New("loader must not run")
}

// TestCluster_SlotMigrationFollowsMOVED moves a populated slot to another
// primary, whose script cache is empty, and checks reads, writes and deletes
// follow the MOVED redirect.
func TestCluster_SlotMigrationFollowsMOVED(t *testing.T) {
	ctx, client, conn, nodes := topologyTest(t)
	keys := taggedKeys(uuid.New().String(), 4)
	slot := keySlot(t, conn, keys[0])
	from := slotOwner(t, nodes, slot)
	to := otherPrimary(t, nodes, from)

	want := valuesFor(keys, "before")
	got, err := client.GetMulti(ctx, time.Minute, keys, exactLoader(want))
	require.NoError(t, err)
	require.Equal(t, want, got)

	migrateSlot(t, nodes, slot, from, to)
	t.Cleanup(func() { migrateSlot(t, nodes, slot, to, from) })
	flushScripts(t, to.addr)
	require.Equal(t, to.id, slotOwner(t, nodes, slot).id)

	got, err = client.GetMulti(ctx, time.Minute, keys, noLoader)
	require.NoError(t, err)
	assert.Equal(t, want, got, "values must survive the move")

	after := valuesFor(keys, "after")
	require.NoError(t, client.SetMulti(ctx, time.Minute, keys, exactLoader(after)))
	got, err = client.GetMulti(ctx, time.Minute, keys, noLoader)
	require.NoError(t, err)
	assert.Equal(t, after, got)
	v, err := do(t, adminClient(t, to.addr), "GET", keys[0]).ToString()
	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(v, ":"+after[keys[0]]), "the write must land on the new owner: %q", v)

	require.NoError(t, client.TouchMulti(ctx, time.Hour, keys))
	require.NoError(t, client.DelMulti(ctx, keys))
	n, err := do(t, adminClient(t, to.addr), "EXISTS", keys[0]).AsInt64()
	require.NoError(t, err)
	assert.Zero(t, n)
}

// TestCluster_SlotMigrationFollowsASK leaves a slot half-migrated: keys that
// still exist on the source are served there, and new keys are redirected to
// the target with ASK. The target's script cache is empty.
func TestCluster_SlotMigrationFollowsASK(t *testing.T) {
	ctx, client, conn, nodes := topologyTest(t)
	tag := uuid.New().String()
	keys := taggedKeys(tag, 3)
	old, fresh := keys[0], keys[1:]
	slot := keySlot(t, conn, old)
	from := slotOwner(t, nodes, slot)
	to := otherPrimary(t, nodes, from)

	want := valuesFor(keys, "load")
	_, err := client.GetMulti(ctx, time.Minute, []string{old}, exactLoader(want))
	require.NoError(t, err)

	beginSlotMigration(t, slot, from, to)
	t.Cleanup(func() {
		finishSlotMigration(t, nodes, slot, from, to)
		migrateSlot(t, nodes, slot, to, from)
	})
	flushScripts(t, to.addr)

	var loaded []string
	got, err := client.GetMulti(ctx, time.Minute, keys, func(c context.Context, ks []string) (map[string]string, error) {
		loaded = append(loaded, ks...)
		return exactLoader(want)(c, ks)
	})
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.ElementsMatch(t, fresh, loaded, "only the new keys are loaded")

	got, err = client.GetMulti(ctx, time.Minute, keys, noLoader)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	// New keys live on the importing node, which serves them only after ASKING.
	dst := adminClient(t, to.addr)
	resps := dst.DoMulti(ctx, dst.B().Arbitrary("ASKING").Build(), dst.B().Get().Key(fresh[0]).Build())
	v, err := resps[1].ToString()
	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(v, ":"+want[fresh[0]]), "new key must be on the target: %q", v)

	after := valuesFor(keys, "after")
	require.NoError(t, client.SetMulti(ctx, time.Minute, keys, exactLoader(after)))
	got, err = client.GetMulti(ctx, time.Minute, keys, noLoader)
	require.NoError(t, err)
	assert.Equal(t, after, got)
	require.NoError(t, client.DelMulti(ctx, keys))
}

// TestCluster_PrimaryFailover freezes a primary with DEBUG SLEEP until its
// replica takes over, then checks the multi-key paths against the promoted
// replica, whose script cache is empty. The frozen node rejoins as a replica
// and is failed back in cleanup.
func TestCluster_PrimaryFailover(t *testing.T) {
	ctx, client, conn, nodes := topologyTest(t)
	keys := taggedKeys(uuid.New().String(), 4)
	victim := slotOwner(t, nodes, keySlot(t, conn, keys[0]))
	var replica clusterNode
	for _, n := range nodes {
		if n.primaryID == victim.id {
			replica = n
		}
	}
	require.NotEmpty(t, replica.id, "victim %s has no replica", victim.addr)

	want := valuesFor(keys, "before")
	_, err := client.GetMulti(ctx, time.Minute, keys, exactLoader(want))
	require.NoError(t, err)
	// Make sure the replica has the values before the primary goes away.
	require.NoError(t, do(t, adminClient(t, victim.addr), "WAIT", "1", "2000").Error())

	frozen := adminClient(t, victim.addr)
	slept := make(chan error, 1)
	go func() {
		slept <- frozen.Do(context.Background(), frozen.B().Arbitrary("DEBUG", "SLEEP", "6").Build()).Error()
	}()
	t.Cleanup(func() {
		<-slept
		eventually(t, 20*time.Second, victim.addr+" to rejoin as a replica", func() bool { return role(t, victim.addr) == "slave" })
		require.NoError(t, do(t, adminClient(t, victim.addr), "CLUSTER", "FAILOVER").Error())
		eventually(t, 20*time.Second, victim.addr+" to be primary again", func() bool { return role(t, victim.addr) == "master" })
		// Later tests read the topology from 30001, so wait for its view too.
		eventually(t, 20*time.Second, "30001 to see the failback", func() bool {
			var victimPrimary, replicaPrimary bool
			for _, n := range clusterNodes(t, "127.0.0.1:30001") {
				switch n.id {
				case victim.id:
					victimPrimary = n.primary
				case replica.id:
					replicaPrimary = n.primary
				}
			}
			return victimPrimary && !replicaPrimary
		})
	})
	eventually(t, 20*time.Second, replica.addr+" to be promoted", func() bool { return role(t, replica.addr) == "master" })

	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	start := time.Now()
	got, err := client.GetMulti(opCtx, time.Minute, keys, noLoader)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	after := valuesFor(keys, "after")
	require.NoError(t, client.SetMulti(opCtx, time.Minute, keys, exactLoader(after)))
	fresh := taggedKeys(uuid.New().String(), 2)
	freshWant := valuesFor(fresh, "load")
	got, err = client.GetMulti(opCtx, time.Minute, fresh, exactLoader(freshWant))
	require.NoError(t, err)
	assert.Equal(t, freshWant, got)
	require.NoError(t, client.DelMulti(opCtx, append(keys, fresh...)))
	t.Logf("multi-key ops after failover took %s", time.Since(start))
}

var cmdstatCalls = regexp.MustCompile(`(?m)^cmdstat_(script\|load|evalsha|eval):calls=(\d+)`)

func scriptCalls(t *testing.T, addr string) map[string]int {
	t.Helper()
	info, err := do(t, adminClient(t, addr), "INFO", "commandstats").ToString()
	require.NoError(t, err)
	out := make(map[string]int)
	for _, m := range cmdstatCalls.FindAllStringSubmatch(info, -1) {
		n, err := strconv.Atoi(m[2])
		require.NoError(t, err)
		out[m[1]] = n
	}
	return out
}

// TestCluster_NoScriptLoad checks the multi-key paths send no SCRIPT LOAD to
// any node, and no scripts at all to replicas. rueidis's Lua.ExecMulti sent
// one SCRIPT LOAD per call to every node, replicas included.
func TestCluster_NoScriptLoad(t *testing.T) {
	ctx, client, conn, nodes := topologyTest(t)
	keys := clusterKeys(t)
	requireSpansPrimaries(t, conn, keys)
	before := make(map[string]map[string]int, len(nodes))
	for _, n := range nodes {
		before[n.addr] = scriptCalls(t, n.addr)
	}

	want := valuesFor(keys, "load")
	_, err := client.GetMulti(ctx, time.Minute, keys, exactLoader(want))
	require.NoError(t, err)
	require.NoError(t, client.SetMulti(ctx, time.Minute, keys, exactLoader(valuesFor(keys, "set"))))
	require.NoError(t, client.TouchMulti(ctx, time.Hour, keys))

	var primaryScripts int
	for _, n := range nodes {
		after := scriptCalls(t, n.addr)
		assert.Equal(t, before[n.addr]["script|load"], after["script|load"], "SCRIPT LOAD on %s", n.addr)
		ran := after["evalsha"] + after["eval"] - before[n.addr]["evalsha"] - before[n.addr]["eval"]
		// ROLE is the node's own answer; CLUSTER NODES can lag a failover.
		if role(t, n.addr) == "master" {
			primaryScripts += ran
		} else {
			assert.Zero(t, ran, "scripts on replica %s", n.addr)
		}
	}
	assert.Positive(t, primaryScripts)
}
