#!/bin/sh
# Starts a 3-primary, 3-replica Redis Cluster on ports 30001-30006 inside one
# container, announcing 127.0.0.1 so clients outside the container can follow
# MOVED redirects through the published ports. Used by docker-compose.yml.
# A 2s node timeout and DEBUG let cluster_topology_test.go freeze a primary and
# see its replica take over within a few seconds.
set -eu

ports="30001 30002 30003 30004 30005 30006"
for port in $ports; do
	# Start empty: /data is a volume that outlives the container, and a stale
	# nodes.conf makes `--cluster create` refuse to run.
	rm -rf "/data/$port"
	mkdir -p "/data/$port"
	redis-server --port "$port" --dir "/data/$port" --daemonize yes \
		--cluster-enabled yes --cluster-config-file nodes.conf \
		--cluster-announce-ip 127.0.0.1 --cluster-node-timeout 2000 \
		--enable-debug-command yes --appendonly no --save ''
done
for port in $ports; do
	until redis-cli -p "$port" ping >/dev/null 2>&1; do sleep 0.1; done
done

if ! redis-cli -p 30001 cluster info | grep -q 'cluster_state:ok'; then
	# shellcheck disable=SC2046 # word splitting builds the node list.
	redis-cli --cluster create $(for p in $ports; do printf '127.0.0.1:%s ' "$p"; done) \
		--cluster-replicas 1 --cluster-yes
fi

exec tail -f /dev/null
