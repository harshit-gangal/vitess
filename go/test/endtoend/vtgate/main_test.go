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

package vtgate

import (
	"flag"
	"os"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	KeyspaceName    = "ks"
	Cell            = "test"
	SchemaSQL       = `CREATE TABLE txn_info (
  id bigint(20) NOT NULL,
  txn_id varchar(50) NOT NULL,
  org_txn_id varchar(50) DEFAULT NULL,
  request_id varchar(50) NOT NULL,
  channel varchar(50) NOT NULL,
  rrn varchar(20) NOT NULL,
  extended_info json DEFAULT NULL,
  created_on timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_on timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY txn_id (txn_id),
  UNIQUE KEY request_id (request_id,channel),
  KEY org_txn_id (org_txn_id),
  KEY rrn (rrn)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE orgTxnId_id_vdx (
  org_txn_id varchar(50) NOT NULL,
  id bigint(20) NOT NULL,
  keyspace_id varbinary(50) NOT NULL,
  PRIMARY KEY (org_txn_id,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE reqid_channel_key_vdx (
  request_id varchar(50) NOT NULL,
  channel varchar(50) NOT NULL,
  keyspace_id varbinary(50) NOT NULL,
  PRIMARY KEY (request_id,channel)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

	VSchema = `
{
  "sharded": true,
  "vindexes": {
    "unicode_loose_md5_vdx": {
      "type": "unicode_loose_md5"
    },
    "reqid_channel_key_vdx": {
      "type": "consistent_lookup",
      "params": {
        "table": "reqid_channel_key_vdx",
        "from": "request_id, channel",
        "to": "keyspace_id",
        "autocommit": "true"
      },
      "owner": "txn_info"
    },
    "orgTxnId_id_vdx": {
      "type": "consistent_lookup",
      "params": {
        "table": "orgTxnId_id_vdx",
        "from": "org_txn_id, id",
        "to": "keyspace_id",
        "autocommit": "true"
      },
      "owner": "txn_info"
    }
  },
  "tables": {
    "txn_info": {
      "column_vindexes": [
        {
          "column": "txn_id",
          "name": "unicode_loose_md5_vdx"
        },
        {
          "columns": [
            "request_id",
            "channel"
          ],
          "name": "reqid_channel_key_vdx"
        },
        {
          "columns": [
            "org_txn_id",
            "id"
          ],
          "name": "orgTxnId_id_vdx"
        }
      ],
      "columns" : [
        { 
           "name" : "org_txn_id",
           "type" : "VARCHAR"
		}
	  ]
    },
    "reqid_channel_key_vdx": {
      "column_vindexes": [
        {
          "column": "request_id",
          "name": "unicode_loose_md5_vdx"
        }
      ]
    },
    "orgTxnId_id_vdx": {
      "column_vindexes": [
        {
          "column": "org_txn_id",
          "name": "unicode_loose_md5_vdx"
        }
      ]
    }
  }
}	`
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(Cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      KeyspaceName,
			SchemaSQL: SchemaSQL,
			VSchema:   VSchema,
		}
		err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 1, true)
		if err != nil {
			return 1
		}

		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}
