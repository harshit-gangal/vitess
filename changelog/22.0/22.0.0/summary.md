## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Deprecations and Deletions](#deprecations-and-deletions)**
    - [Deprecated VTTablet Flags](#vttablet-flags)
  - **[RPC Changes](#rpc-changes)**
  - **[Prefer not promoting a replica that is currently taking a backup](#reparents-prefer-not-backing-up)**
  - **[VTOrc Config File Changes](#vtorc-config-file-changes)**
  - **[VTGate Config File Changes](#vtgate-config-file-changes)**
  - **[Support for More Efficient JSON Replication](#efficient-json-replication)**
  - **[Support for LAST_INSERT_ID(x)](#last-insert-id)**
  - **[Support for Maximum Idle Connections in the Pool](#max-idle-connections)**
- **[Minor Changes](#minor-changes)**
  - **[VTTablet Flags](#flags-vttablet)**
  - **[Topology read concurrency behaviour changes](#topo-read-concurrency-changes)**

## <a id="major-changes"/>Major Changes</a>

### <a id="rpc-changes"/>RPC Changes</a>

These are the RPC changes made in this release - 

1. `GetTransactionInfo` RPC has been added to both `VtctldServer`, and `TabletManagerClient` interface. These RPCs are used to fecilitate the users in reading the state of an unresolved distributed transaction. This can be useful in debugging what went wrong and how to fix the problem.

### <a id="deprecations-and-deletions"/>Deprecations and Deletions</a>

#### <a id="vttablet-flags"/>Deprecated VTTablet Flags</a>

- `twopc_enable` flag is deprecated. Usage of TwoPC commit will be determined by the `transaction_mode` set on VTGate via flag or session variable.

### <a id="reparents-prefer-not-backing-up"/>Prefer not promoting a replica that is currently taking a backup

Emergency reparents now prefer not promoting replicas that are currently taking backups with a backup engine other than
`builtin`. Note that if there's only one suitable replica to promote, and it is taking a backup, it will still be
promoted.

For planned reparents, hosts taking backups with a backup engine other than `builtin` are filtered out of the list of
valid candidates. This means they will never get promoted - not even if there's no other candidates.

Note that behavior for `builtin` backups remains unchanged: a replica that is currently taking a `builtin` backup will
never be promoted, neither by planned nor by emergency reparents.

### <a id="vtorc-config-file-changes"/>VTOrc Config File Changes</a>

The configuration file for VTOrc has been updated to now support dynamic fields. The old `--config` parameter has been removed. The alternative is to use the `--config-file` parameter. The configuration can now be provided in json, yaml or any other format that [viper](https://github.com/spf13/viper) supports.

The following fields can be dynamically changed - 
1. `instance-poll-time`
2. `prevent-cross-cell-failover`
3. `snapshot-topology-interval`
4. `reasonable-replication-lag`
5. `audit-to-backend`
6. `audit-to-syslog`
7. `audit-purge-duration`
8. `wait-replicas-timeout`
9. `tolerable-replication-lag`
10. `topo-information-refresh-duration`
11. `recovery-poll-duration`
12. `allow-emergency-reparent`
13. `change-tablets-with-errant-gtid-to-drained`

To upgrade to the newer version of the configuration file, first switch to using the flags in your current deployment before upgrading. Then you can switch to using the configuration file in the newer release.

### <a id="vtgate-config-file-changes"/>VTGate Config File Changes</a>

The Viper configuration keys for the following flags has been changed to match their flag names. Previously they had a discovery prefix instead of it being part of the name. 

| Flag Name                                        | Old Configuration Key                            | New Configuration Key                            |
|--------------------------------------------------|--------------------------------------------------|--------------------------------------------------|
| `discovery_low_replication_lag`                  | `discovery.low_replication_lag`                  | `discovery_low_replication_lag`                  |
| `discovery_high_replication_lag_minimum_serving` | `discovery.high_replication_lag_minimum_serving` | `discovery_high_replication_lag_minimum_serving` |
| `discovery_min_number_serving_vttablets`         | `discovery.min_number_serving_vttablets`         | `discovery_min_number_serving_vttablets`         |
| `discovery_legacy_replication_lag_algorithm`     | `discovery.legacy_replication_lag_algorithm`     | `discovery_legacy_replication_lag_algorithm`     |

To upgrade to the newer version of the configuration keys, first switch to using the flags in your current deployment before upgrading. Then you can switch to using the new configuration keys in the newer release.

### <a id="efficient-json-replication"/>Support for More Efficient JSON Replication</a>

In [#7345](https://github.com/vitessio/vitess/pull/17345) we added support for [`--binlog-row-value-options=PARTIAL_JSON`](https://dev.mysql.com/doc/refman/en/replication-options-binary-log.html#sysvar_binlog_row_value_options). You can read more about [this feature added to MySQL 8.0 here](https://dev.mysql.com/blog-archive/efficient-json-replication-in-mysql-8-0/).

If you are using MySQL 8.0 or later and using JSON columns, you can now enable this MySQL feature across your Vitess cluster(s) to lower the disk space needed for binary logs and improve the CPU and memory usage in both `mysqld` (standard intrashard MySQL replication) and `vttablet` ([VReplication](https://vitess.io/docs/reference/vreplication/vreplication/)) without losing any capabilities or features.

### <a id="last-insert-id"/>Support for `LAST_INSERT_ID(x)`</a>

In [#17408](https://github.com/vitessio/vitess/pull/17408) and [#17409](https://github.com/vitessio/vitess/pull/17409), we added the ability to use `LAST_INSERT_ID(x)` in Vitess directly at vtgate. This improvement allows certain queries—like `SELECT last_insert_id(123);` or `SELECT last_insert_id(count(*)) ...`—to be handled without relying on MySQL for the final value.

**Limitations**:
- When using `LAST_INSERT_ID(x)` in ordered queries (e.g., `SELECT last_insert_id(col) FROM table ORDER BY foo`), MySQL sets the session’s last-insert-id value according to the *last row returned*. Vitess does not guarantee the same behavior.

### <a id="max-idle-connections"/>Support for Maximum Idle Connections in the Pool</a>

In [#17443](https://github.com/vitessio/vitess/pull/17443) we introduced a new configurable max-idle-count parameter for connection pools. This allows you to specify the maximum number of idle connections retained in each connection pool to optimize performance and resource efficiency.

You can control idle connection retention for the query server’s query pool, stream pool, and transaction pool with the following flags:
•	--queryserver-config-query-pool-max-idle-count: Defines the maximum number of idle connections retained in the query pool.
•	--queryserver-config-stream-pool-max-idle-count: Defines the maximum number of idle connections retained in the stream pool.
•	--queryserver-config-txpool-max-idle-count: Defines the maximum number of idle connections retained in the transaction pool.

This feature ensures that, during traffic spikes, idle connections are available for faster responses, while minimizing overhead in low-traffic periods by limiting the number of idle connections retained. It helps strike a balance between performance, efficiency, and cost.

## <a id="minor-changes"/>Minor Changes</a>

#### <a id="flags-vttablet"/>VTTablet Flags</a>

- `twopc_abandon_age` flag now supports values in the time.Duration format (e.g., 1s, 2m, 1h). 
While the flag will continue to accept float values (interpreted as seconds) for backward compatibility, 
**float inputs are deprecated** and will be removed in a future release.

### <a id="topo-read-concurrency-changes"/>`--topo_read_concurrency` behaviour changes

The `--topo_read_concurrency` flag was added to all components that access the topology and the provided limit is now applied separately for each global or local cell _(default `32`)_.

All topology read calls _(`Get`, `GetVersion`, `List` and `ListDir`)_ now respect this per-cell limit. Previous to this version a single limit was applied to all cell calls and it was not respected by many topology calls.
