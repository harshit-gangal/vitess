package tabletserver

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tx"
)

func TestStatefulConnection_RenewPass(t *testing.T) {
	conn := StatefulConnection{
		pool:           &fakeStatefulConnPool{},
		ConnID:         1,
		env:            nil,
		txProps:        nil,
		tainted:        false,
		enforceTimeout: false,
	}
	err := conn.Renew()
	assert.NoError(t, err)
	assert.EqualValues(t, 2, conn.ConnID)
}

func TestStatefulConnection_RenewFail(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	connPool := newPool()
	connPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	defer connPool.Close()
	dbConn, err := connpool.NewDBConn(context.Background(), connPool, db.ConnParams())
	if dbConn != nil {
		defer dbConn.Close()
	}

	conn := StatefulConnection{
		pool:   &fakeStatefulConnPool{renewErr: "some error"},
		ConnID: 1,
		dbConn: dbConn,
	}
	err = conn.Renew()
	assert.Containsf(t, err.Error(), "connection renew failed", "")
	assert.EqualValues(t, 1, conn.ConnID)
	assert.True(t, conn.IsClosed())
}

func newPool() *connpool.Pool {
	return connpool.NewPool(tabletenv.NewEnv(nil, "PoolTest"), "TestPool", tabletenv.ConnPoolConfig{
		Size:               100,
		IdleTimeoutSeconds: 10,
	})
}

var _ IStatefulConnPool = (*fakeStatefulConnPool)(nil)

type fakeStatefulConnPool struct {
	renewErr string
}

func (f fakeStatefulConnPool) markAsNotInUse(id tx.ConnID) {
	panic("implement me")
}

func (f fakeStatefulConnPool) unregister(id tx.ConnID, reason string) {
	panic("implement me")
}

func (f fakeStatefulConnPool) renewConn(sc *StatefulConnection) error {
	if f.renewErr != "" {
		return errors.New(f.renewErr)
	}
	sc.ConnID = sc.ConnID + 1
	return nil
}
