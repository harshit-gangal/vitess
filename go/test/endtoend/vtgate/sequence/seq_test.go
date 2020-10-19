/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sequence

import (
	"context"
	"flag"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance    *cluster.LocalProcessCluster
	cell               = "zone1"
	hostname           = "localhost"
	unshardedKs        = "uks"
	unshardedSQLSchema = `

CREATE TABLE user_seq ( id INT, next_id BIGINT, cache BIGINT, PRIMARY KEY(id)) comment 'vitess_sequence';
INSERT INTO user_seq (id, next_id, cache) values (0, 1, 1000);
CREATE TABLE user_order_seq ( id INT, next_id BIGINT, cache BIGINT, PRIMARY KEY(id)) comment 'vitess_sequence';
INSERT INTO user_order_seq (id, next_id, cache) values (0, 1, 1000);
CREATE TABLE order_items_seq ( id INT, next_id BIGINT, cache BIGINT, PRIMARY KEY(id)) comment 'vitess_sequence';
INSERT INTO order_items_seq (id, next_id, cache) values (0, 1, 1000);

	`

	unshardedVSchema = `
		{	
			"sharded":false,
			"tables": {
                "user_seq": {
                    "type": "sequence"
                 },
                "user_order_seq": {
                    "type": "sequence"
                 },
                "order_items_seq": {
                    "type": "sequence"
                 }
			}
		}
`

	shardedKeyspaceName = `sks`

	shardedSQLSchema = `
create table user (id bigint, name varchar(50), email varchar(100), primary key (id));


create table user_order (id bigint, user_id bigint, order_date timestamp, primary key (id));


create table order_items (id bigint, item_id bigint, order_id bigint, quantity bigint, primary key (id), unique key(item_id, order_id));


create table order_user_lookup(order_id bigint, keyspace_id varbinary(50), primary key(order_id));

`
	shardedVSchema = `
		{
		  "sharded": true,
		  "vindexes": {
			"lookup_vindex": {
			  "type": "consistent_lookup_unique",
			  "params": {
				"table": "order_user_lookup",
				"from": "order_id",
				"to": "keyspace_id"
			  },
			  "owner": "user_order"
			},
			"xxhash": {
			  "type": "xxhash"
			}
		  },
		  "tables": {
			"user": {
			  "columnVindexes": [
				{
				  "column": "id",
				  "name": "xxhash"
				}
			  ],
			  "autoIncrement": {
				"column": "id",
				"sequence": "user_seq"
			  }
			},
			"user_order": {
			  "columnVindexes": [
				{
				  "column": "user_id",
				  "name": "xxhash"
				},
				{
				  "name": "lookup_vindex",
				  "columns": [ "id" ]
				}
			  ],
			  "autoIncrement": {
				"column": "id",
				"sequence": "user_order_seq"
			  }
			},
			"order_user_lookup": {
			  "columnVindexes": [
				{
				  "column": "order_id",
				  "name": "xxhash"
				}
			  ]
			},
			"order_items": {
			  "columnVindexes": [
				{
				  "column": "order_id",
				  "name": "lookup_vindex"
				}
			  ],
			  "autoIncrement": {
				"column": "id",
				"sequence": "order_items_seq"
			  }
			}
		  }
		}
`

)

func TestSeq(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	//Initialize User table
	exec(t, conn, `insert into user(name, email) values('John', 'john@xyz.com'), ('Emma', 'emma@xyz.com'), ('Clark', 'clark@xyz.com')`)

	//Insert order for John
	exec(t, conn, `begin`)
	exec(t, conn, `insert into user_order(user_id, order_date) values(1, '2020-10-19')`)
	exec(t, conn, `insert into order_items(item_id, order_id, quantity) values(990, 1, 24)`)
	exec(t, conn, `commit`)

	//exec(t, conn, `select uo.user_id, sum(io.quantity) from user_order uo join order_items oi on uo.order_id = oi.order_id order by uo`)
	//exec(t, conn, `select u.name, u.email from user u
	//						where u.id = (select t.uid, max(t.uq) from
	//						(select uo.user_id as uid, sum(io.quantity) as uq
	//							from user_order uo join order_items oi on uo.order_id = oi.order_id
	//							order by uo.user_id) t order by t.uid)`)
}

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Start keyspace
		uKeyspace := &cluster.Keyspace{
			Name:      unshardedKs,
			SchemaSQL: unshardedSQLSchema,
			VSchema:   unshardedVSchema,
		}
		if err := clusterInstance.StartUnshardedKeyspace(*uKeyspace, 0, false); err != nil {
			return 1
		}

		sKeyspace := &cluster.Keyspace{
			Name:      shardedKeyspaceName,
			SchemaSQL: shardedSQLSchema,
			VSchema:   shardedVSchema,
		}
		if err := clusterInstance.StartKeyspace(*sKeyspace, []string{"-80", "80-"}, 0, false); err != nil {
			return 1
		}

		// Start vtgate
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.Nil(t, err)
	return qr
}
