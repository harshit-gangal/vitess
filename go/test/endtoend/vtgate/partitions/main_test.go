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
	"context"
	_ "embed"
	"flag"
	"github.com/stretchr/testify/require"
	"os"
	"sync"
	"testing"
	"vitess.io/vitess/go/test/endtoend/utils"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	KeyspaceName    = "ks"
	Cell            = "test"
	//go:embed schema.sql
	SchemaSQL string

	//go:embed vschema.json
	VSchema string
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
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
		err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 0, false)
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

func TestPartitioning(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	_ = utils.Exec(t, conn, `insert into messages (send_id, status_id, subscriber_id, created_at) values ("1234", 1, 1, '2022-06-15'), ("1234", 1, 1, '2022-06-10'), ("1234", 1, 1, '2022-06-20')`)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			nConn, err := mysql.Connect(context.Background(), &vtParams)
			require.NoError(t, err)
			defer nConn.Close()

			_ = utils.Exec(t, nConn, `insert into messages (send_id, status_id, subscriber_id, created_at) values ("1234", 1, 1, '2022-06-15'), ("abcd", 1, 3, '2022-06-15'), ("1234", 1, 1, '2022-06-15'), ("1234", 1, 1, '2022-06-20'), ("abcd", 1, 3, '2022-06-20'), ("1234", 1, 1, '2022-06-20'), ("1234", 1, 1, '2022-06-25'), ("abcd", 1, 3, '2022-06-25'), ("1234", 1, 1, '2022-06-25') on duplicate key update status_id = 2`)
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestPartitioning2(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	_ = utils.Exec(t, conn, `insert into messages (send_id, status_id, subscriber_id) values ("1234", 1, 1)`)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			nConn, err := mysql.Connect(context.Background(), &vtParams)
			require.NoError(t, err)
			defer nConn.Close()

			_ = utils.Exec(t, nConn, `insert into messages (send_id, status_id, subscriber_id) values ("1234", 1, 1), ("abcd", 1, 3), ("1234", 1, 1) on duplicate key update status_id = 2`)
			wg.Done()
		}()
	}
	wg.Wait()
}
