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

package connpool

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/pools/smartconnpool"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

func compareTimingCounts(t *testing.T, op string, delta int64, before, after map[string]int64) {
	t.Helper()
	countBefore := before[op]
	countAfter := after[op]
	if countAfter-countBefore != delta {
		t.Errorf("Expected %s to increase by %d, got %d (%d => %d)", op, delta, countAfter-countBefore, countBefore, countAfter)
	}
}

func TestDBConnExec(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	sql := "select * from test_table limit 1000"
	expectedResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarChar},
		},
		RowsAffected: 0,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar("123")},
		},
	}
	db.AddQuery(sql, expectedResult)
	connPool := newPool()
	mysqlTimings := connPool.env.Stats().MySQLTimings
	startCounts := mysqlTimings.Counts()
	params := dbconfigs.New(db.ConnParams())
	connPool.Open(params, params, params)
	defer connPool.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer cancel()
	dbConn, err := newPooledConn(context.Background(), connPool, params)
	if dbConn != nil {
		defer dbConn.Close()
	}
	require.NoError(t, err)
	// Exec succeed, not asking for fields.
	result, err := dbConn.Exec(ctx, sql, 1, false)
	require.NoError(t, err)
	expectedResult.Fields = nil
	require.True(t, expectedResult.Equal(result))

	compareTimingCounts(t, "PoolTest.Exec", 1, startCounts, mysqlTimings.Counts())

	startCounts = mysqlTimings.Counts()

	// Exec fail due to client side error
	db.AddRejectedQuery(sql, &sqlerror.SQLError{
		Num:     2012,
		Message: "connection fail",
		Query:   "",
	})
	_, err = dbConn.Exec(ctx, sql, 1, false)
	require.Error(t, err)
	require.ErrorContains(t, err, "connection fail")

	// The client side error triggers a retry in exec.
	compareTimingCounts(t, "PoolTest.Exec", 2, startCounts, mysqlTimings.Counts())

	startCounts = mysqlTimings.Counts()

	// Set the connection fail flag and try again.
	// This time the initial query fails as does the reconnect attempt.
	db.EnableConnFail()
	_, err = dbConn.Exec(ctx, sql, 1, false)
	require.Error(t, err)
	require.ErrorContains(t, err, "packet read failed")
	db.DisableConnFail()

	compareTimingCounts(t, "PoolTest.Exec", 1, startCounts, mysqlTimings.Counts())
}

func TestDBConnExecLost(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	sql := "select * from test_table limit 1000"
	expectedResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarChar},
		},
		RowsAffected: 0,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar("123")},
		},
	}
	db.AddQuery(sql, expectedResult)
	connPool := newPool()
	mysqlTimings := connPool.env.Stats().MySQLTimings
	startCounts := mysqlTimings.Counts()
	params := dbconfigs.New(db.ConnParams())
	connPool.Open(params, params, params)
	defer connPool.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer cancel()
	dbConn, err := newPooledConn(context.Background(), connPool, params)
	if dbConn != nil {
		defer dbConn.Close()
	}
	require.NoError(t, err)
	// Exec succeed, not asking for fields.
	result, err := dbConn.Exec(ctx, sql, 1, false)
	require.NoError(t, err)
	expectedResult.Fields = nil
	if !expectedResult.Equal(result) {
		t.Errorf("Exec: %v, want %v", expectedResult, result)
	}

	compareTimingCounts(t, "PoolTest.Exec", 1, startCounts, mysqlTimings.Counts())

	// Exec fail due to server side error (e.g. query kill)
	startCounts = mysqlTimings.Counts()
	db.AddRejectedQuery(sql, &sqlerror.SQLError{
		Num:     2013,
		Message: "Lost connection to MySQL server during query",
		Query:   "",
	})
	_, err = dbConn.Exec(ctx, sql, 1, false)
	require.Error(t, err)
	require.ErrorContains(t, err, "Lost connection to MySQL server during query")

	// Should *not* see a retry, so only increment by 1
	compareTimingCounts(t, "PoolTest.Exec", 1, startCounts, mysqlTimings.Counts())
}

func TestDBConnDeadline(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	sql := "select * from test_table limit 1000"
	expectedResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarChar},
		},
		RowsAffected: 0,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar("123")},
		},
	}
	db.AddQuery(sql, expectedResult)

	connPool := newPool()
	mysqlTimings := connPool.env.Stats().MySQLTimings
	startCounts := mysqlTimings.Counts()
	params := dbconfigs.New(db.ConnParams())
	connPool.Open(params, params, params)
	defer connPool.Close()

	db.SetConnDelay(100 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(50*time.Millisecond))
	defer cancel()

	dbConn, err := newPooledConn(context.Background(), connPool, params)
	if dbConn != nil {
		defer dbConn.Close()
	}
	require.NoError(t, err)

	_, err = dbConn.Exec(ctx, sql, 1, false)
	require.Error(t, err)
	require.ErrorContains(t, err, "(errno 3024) (sqlstate HY000): Query execution was interrupted, maximum statement execution time exceeded before execution started")

	compareTimingCounts(t, "PoolTest.Exec", 0, startCounts, mysqlTimings.Counts())

	startCounts = mysqlTimings.Counts()

	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer cancel()

	result, err := dbConn.Exec(ctx, sql, 1, false)
	require.NoError(t, err)
	expectedResult.Fields = nil
	if !expectedResult.Equal(result) {
		t.Errorf("Exec: %v, want %v", expectedResult, result)
	}

	compareTimingCounts(t, "PoolTest.Exec", 1, startCounts, mysqlTimings.Counts())

	startCounts = mysqlTimings.Counts()

	// Test with just the Background context (with no deadline)
	result, err = dbConn.Exec(context.Background(), sql, 1, false)
	require.NoError(t, err)
	expectedResult.Fields = nil
	if !expectedResult.Equal(result) {
		t.Errorf("Exec: %v, want %v", expectedResult, result)
	}

	compareTimingCounts(t, "PoolTest.Exec", 1, startCounts, mysqlTimings.Counts())
}

func TestDBConnKill(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	connPool := newPool()
	params := dbconfigs.New(db.ConnParams())
	connPool.Open(params, params, params)
	defer connPool.Close()
	dbConn, err := newPooledConn(context.Background(), connPool, params)
	if dbConn != nil {
		defer dbConn.Close()
	}
	require.NoError(t, err)
	query := fmt.Sprintf("kill %d", dbConn.ID())
	db.AddQuery(query, &sqltypes.Result{})
	// Kill failed because we are not able to connect to the database
	db.EnableConnFail()
	err = dbConn.Kill("test kill", 0)
	require.Error(t, err)
	require.ErrorContains(t, err, "errno 2013")
	db.DisableConnFail()

	// Kill succeed
	err = dbConn.Kill("test kill", 0)
	if err != nil {
		t.Fatalf("kill should succeed, but got error: %v", err)
	}

	err = dbConn.Reconnect(context.Background())
	if err != nil {
		t.Fatalf("reconnect should succeed, but got error: %v", err)
	}
	newKillQuery := fmt.Sprintf("kill %d", dbConn.ID())
	// Kill failed because "kill query_id" failed
	db.AddRejectedQuery(newKillQuery, errors.New("rejected"))
	err = dbConn.Kill("test kill", 0)
	require.Error(t, err)
	require.ErrorContains(t, err, "rejected")
}

func TestDBKillWithContext(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	connPool := newPool()
	params := dbconfigs.New(db.ConnParams())
	connPool.Open(params, params, params)
	defer connPool.Close()
	dbConn, err := newPooledConn(context.Background(), connPool, params)
	if dbConn != nil {
		defer dbConn.Close()
	}
	require.NoError(t, err)

	query := fmt.Sprintf("kill %d", dbConn.ID())
	db.AddQuery(query, &sqltypes.Result{})
	db.SetBeforeFunc(query, func() {
		// should take longer than our context deadline below.
		time.Sleep(200 * time.Millisecond)
	})

	// set a lower timeout value
	dbConn.killTimeout = 100 * time.Millisecond

	// Kill should return context.DeadlineExceeded
	err = dbConn.Kill("test kill", 0)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestDBConnCtxError tests that an Exec returns with appropriate error code.
// Also, verifies that does it wait for the query to finish before returning.
func TestDBConnCtxError(t *testing.T) {
	exec := func(ctx context.Context, query string, dbconn *Conn) error {
		_, err := dbconn.Exec(ctx, query, 1, false)
		return err
	}

	execOnce := func(ctx context.Context, query string, dbconn *Conn) error {
		_, err := dbconn.ExecOnce(ctx, query, 1, false)
		return err
	}

	t.Run("context cancel - non-tx exec", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()
		testContextError(t, ctx, exec,
			"(errno 1317) (sqlstate 70100): Query execution was interrupted",
			150*time.Millisecond)
	})

	t.Run("context deadline - non-tx exec", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		testContextError(t, ctx, exec,
			"(errno 3024) (sqlstate HY000): Query execution was interrupted, maximum statement execution time exceeded",
			150*time.Millisecond)
	})

	t.Run("context cancel - tx exec", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()
		testContextError(t, ctx, execOnce,
			"(errno 1317) (sqlstate 70100): Query execution was interrupted",
			50*time.Millisecond)
	})

	t.Run("context deadline - tx exec", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		testContextError(t, ctx, execOnce,
			"(errno 3024) (sqlstate HY000): Query execution was interrupted, maximum statement execution time exceeded",
			50*time.Millisecond)
	})
}

var alloc = func() *sqltypes.Result {
	return &sqltypes.Result{}
}

// TestDBConnStreamCtxError tests that an StreamExec returns with appropriate error code.
// Also, verifies that does it wait for the query to finish before returning.
func TestDBConnStreamCtxError(t *testing.T) {
	exec := func(ctx context.Context, query string, dbconn *Conn) error {
		return dbconn.Stream(ctx, query, func(result *sqltypes.Result) error {
			return nil
		}, alloc, 1, querypb.ExecuteOptions_ALL)
	}

	execOnce := func(ctx context.Context, query string, dbconn *Conn) error {
		return dbconn.StreamOnce(ctx, query, func(result *sqltypes.Result) error {
			return nil
		}, alloc, 1, querypb.ExecuteOptions_ALL)
	}

	t.Run("context cancel - non-tx exec", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()
		testContextError(t, ctx, exec,
			"(errno 1317) (sqlstate 70100): Query execution was interrupted",
			150*time.Millisecond)
	})

	t.Run("context deadline - non-tx exec", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		testContextError(t, ctx, exec,
			"(errno 3024) (sqlstate HY000): Query execution was interrupted, maximum statement execution time exceeded",
			150*time.Millisecond)
	})

	t.Run("context cancel - tx exec", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()
		testContextError(t, ctx, execOnce,
			"(errno 1317) (sqlstate 70100): Query execution was interrupted",
			50*time.Millisecond)
	})

	t.Run("context deadline - tx exec", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		testContextError(t, ctx, execOnce,
			"(errno 3024) (sqlstate HY000): Query execution was interrupted, maximum statement execution time exceeded",
			50*time.Millisecond)
	})
}

func testContextError(t *testing.T,
	ctx context.Context,
	exec func(context.Context, string, *Conn) error,
	expErrMsg string,
	expDuration time.Duration) {
	db := fakesqldb.New(t)
	defer db.Close()
	connPool := newPool()
	params := dbconfigs.New(db.ConnParams())
	connPool.Open(params, params, params)
	defer connPool.Close()

	query := "sleep"
	db.AddQuery(query, &sqltypes.Result{})
	db.SetBeforeFunc(query, func() {
		time.Sleep(100 * time.Millisecond)
	})
	db.AddQueryPattern(`kill query \d+`, &sqltypes.Result{})
	db.AddQueryPattern(`kill \d+`, &sqltypes.Result{})

	dbConn, err := newPooledConn(context.Background(), connPool, params)
	require.NoError(t, err)
	defer dbConn.Close()

	start := time.Now()
	err = exec(ctx, query, dbConn)
	end := time.Now()
	assert.ErrorContains(t, err, expErrMsg)
	assert.WithinDuration(t, end, start, expDuration)
}

func TestDBNoPoolConnKill(t *testing.T) {
	db := fakesqldb.New(t)
	connPool := newPool()
	params := dbconfigs.New(db.ConnParams())
	connPool.Open(params, params, params)
	defer connPool.Close()
	dbConn, err := NewConn(context.Background(), params, connPool.dbaPool, nil, tabletenv.NewEnv(vtenv.NewTestEnv(), nil, "TestDBNoPoolConnKill"))
	if dbConn != nil {
		defer dbConn.Close()
	}
	require.NoError(t, err)
	query := fmt.Sprintf("kill %d", dbConn.ID())
	db.AddQuery(query, &sqltypes.Result{})
	// Kill failed because we are not able to connect to the database
	db.EnableConnFail()
	err = dbConn.Kill("test kill", 0)
	require.Error(t, err)
	var sqlErr *sqlerror.SQLError
	isSqlErr := errors.As(sqlerror.NewSQLErrorFromError(err), &sqlErr)
	require.True(t, isSqlErr)
	require.EqualValues(t, sqlerror.CRServerLost, sqlErr.Number())
	db.DisableConnFail()

	// Kill succeed
	err = dbConn.Kill("test kill", 0)
	if err != nil {
		t.Fatalf("kill should succeed, but got error: %v", err)
	}

	err = dbConn.Reconnect(context.Background())
	if err != nil {
		t.Fatalf("reconnect should succeed, but got error: %v", err)
	}
	newKillQuery := fmt.Sprintf("kill %d", dbConn.ID())
	// Kill failed because "kill query_id" failed
	db.AddRejectedQuery(newKillQuery, errors.New("rejected"))
	err = dbConn.Kill("test kill", 0)
	require.Error(t, err)
	require.ErrorContains(t, err, "rejected")
}

func TestDBConnStream(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	sql := "select * from test_table limit 1000"
	expectedResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar("123")},
		},
	}
	db.AddQuery(sql, expectedResult)
	connPool := newPool()
	params := dbconfigs.New(db.ConnParams())
	connPool.Open(params, params, params)
	defer connPool.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer cancel()
	dbConn, err := newPooledConn(context.Background(), connPool, params)
	if dbConn != nil {
		defer dbConn.Close()
	}
	require.NoError(t, err)
	var result sqltypes.Result
	err = dbConn.Stream(
		ctx, sql, func(r *sqltypes.Result) error {
			// Aggregate Fields and Rows
			if r.Fields != nil {
				result.Fields = r.Fields
			}
			if r.Rows != nil {
				result.Rows = append(result.Rows, r.Rows...)
			}
			return nil
		}, alloc,
		10, querypb.ExecuteOptions_ALL)
	require.NoError(t, err)
	require.True(t, expectedResult.Equal(&result))
	// Stream fail
	db.Close()
	dbConn.Close()
	err = dbConn.Stream(
		ctx, sql, func(r *sqltypes.Result) error {
			return nil
		}, func() *sqltypes.Result {
			return &sqltypes.Result{}
		},
		10, querypb.ExecuteOptions_ALL)
	db.DisableConnFail()
	require.Error(t, err)
	require.ErrorContains(t, err, "no such file or directory (errno 2002)")
}

// TestDBConnKillCall tests that direct Kill method calls work as expected.
func TestDBConnKillCall(t *testing.T) {
	t.Run("stream exec", func(t *testing.T) {
		testKill(t, func(ctx context.Context, query string, dbconn *Conn) error {
			return dbconn.Stream(context.Background(), query,
				func(r *sqltypes.Result) error { return nil },
				alloc, 10, querypb.ExecuteOptions_ALL)
		})
	})

	t.Run("exec", func(t *testing.T) {
		testKill(t, func(ctx context.Context, query string, dbconn *Conn) error {
			_, err := dbconn.Exec(context.Background(), query, 1, false)
			return err
		})
	})
}

func testKill(t *testing.T, exec func(context.Context, string, *Conn) error) {
	db := fakesqldb.New(t)
	defer db.Close()
	sql := "select * from test_table limit 1000"
	expectedResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarChar},
		},
	}
	db.AddQuery(sql, expectedResult)
	db.SetBeforeFunc(sql, func() {
		time.Sleep(100 * time.Millisecond)
	})

	db.AddQueryPattern(`kill query \d+`, &sqltypes.Result{})
	db.AddQueryPattern(`kill \d+`, &sqltypes.Result{})

	connPool := newPool()
	params := dbconfigs.New(db.ConnParams())
	connPool.Open(params, params, params)
	defer connPool.Close()
	dbConn, err := newPooledConn(context.Background(), connPool, params)
	require.NoError(t, err)
	defer dbConn.Close()

	go func() {
		time.Sleep(10 * time.Millisecond)
		dbConn.Kill("kill connection called", 0)
	}()

	err = exec(context.Background(), sql, dbConn)
	assert.ErrorContains(t, err, "kill connection called")
}

func TestDBConnReconnect(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	connPool := newPool()
	params := dbconfigs.New(db.ConnParams())
	connPool.Open(params, params, params)
	defer connPool.Close()

	dbConn, err := newPooledConn(context.Background(), connPool, params)
	require.NoError(t, err)
	defer dbConn.Close()

	oldConnID := dbConn.conn.ID()
	// close the connection and let the dbconn reconnect to start a new connection when required.
	dbConn.conn.Close()

	query := "select 1"
	db.AddQuery(query, &sqltypes.Result{})

	_, err = dbConn.Exec(context.Background(), query, 1, false)
	require.NoError(t, err)
	require.NotEqual(t, oldConnID, dbConn.conn.ID())
}

func TestDBConnReApplySetting(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.OrderMatters()

	connPool := newPool()
	params := dbconfigs.New(db.ConnParams())
	connPool.Open(params, params, params)
	defer connPool.Close()

	ctx := context.Background()
	dbConn, err := newPooledConn(ctx, connPool, params)
	require.NoError(t, err)
	defer dbConn.Close()

	// apply system settings.
	setQ := "set @@sql_mode='ANSI_QUOTES'"
	db.AddExpectedQuery(setQ, nil)
	err = dbConn.ApplySetting(ctx, smartconnpool.NewSetting(setQ, "set @@sql_mode = default"))
	require.NoError(t, err)

	// close the connection and let the dbconn reconnect to start a new connection when required.
	oldConnID := dbConn.conn.ID()
	dbConn.conn.Close()

	// new conn should also have the same settings.
	// set query will be executed first on the new connection and then the query.
	db.AddExpectedQuery(setQ, nil)
	query := "select 1"
	db.AddExpectedQuery(query, nil)
	_, err = dbConn.Exec(ctx, query, 1, false)
	require.NoError(t, err)
	require.NotEqual(t, oldConnID, dbConn.conn.ID())

	db.VerifyAllExecutedOrFail()
}

func TestDBExecOnceKillTimeout(t *testing.T) {
	executeWithTimeout(t, `kill \d+`, 150*time.Millisecond, func(ctx context.Context, dbConn *Conn) (*sqltypes.Result, error) {
		return dbConn.ExecOnce(ctx, "select 1", 1, false)
	})
}

func TestDBExecKillTimeout(t *testing.T) {
	executeWithTimeout(t, `kill query \d+`, 1000*time.Millisecond, func(ctx context.Context, dbConn *Conn) (*sqltypes.Result, error) {
		return dbConn.Exec(ctx, "select 1", 1, false)
	})
}

func executeWithTimeout(
	t *testing.T,
	expectedKillQuery string,
	responseTime time.Duration,
	execute func(context.Context, *Conn) (*sqltypes.Result, error),
) {
	db := fakesqldb.New(t)
	defer db.Close()
	connPool := newPool()
	params := dbconfigs.New(db.ConnParams())
	connPool.Open(params, params, params)
	defer connPool.Close()
	dbConn, err := newPooledConn(context.Background(), connPool, params)
	if dbConn != nil {
		defer dbConn.Close()
	}
	require.NoError(t, err)

	// A very long running query that will be killed.
	expectedQuery := "select 1"
	var timestampQuery atomic.Int64
	db.AddQuery(expectedQuery, &sqltypes.Result{})
	db.SetBeforeFunc(expectedQuery, func() {
		timestampQuery.Store(time.Now().UnixMicro())
		// should take longer than our context deadline below.
		time.Sleep(1000 * time.Millisecond)
	})

	// We expect a kill-query to be fired, too.
	// It should also run into a timeout.
	var timestampKill atomic.Int64
	dbConn.killTimeout = 100 * time.Millisecond

	db.AddQueryPatternWithCallback(expectedKillQuery, &sqltypes.Result{}, func(string) {
		timestampKill.Store(time.Now().UnixMicro())
		// should take longer than the configured kill timeout above.
		time.Sleep(200 * time.Millisecond)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	result, err := execute(ctx, dbConn)
	timeDone := time.Now()

	require.Error(t, err)
	require.Equal(t, vtrpcpb.Code_CANCELED, vterrors.Code(err))
	require.Nil(t, result)
	timeQuery := time.UnixMicro(timestampQuery.Load())
	timeKill := time.UnixMicro(timestampKill.Load())
	// In this unit test, the execution of `select 1` is blocked for 1000ms.
	// The kill query gets executed after 100ms but waits for the query to return which will happen after 1000ms due to the test framework.
	// In real scenario mysql will kill the query immediately and return the error.
	// Here, kill call happens after 100ms but took 1000ms to complete.
	require.WithinDuration(t, timeQuery, timeKill, 150*time.Millisecond)
	require.WithinDuration(t, timeKill, timeDone, responseTime)
}
