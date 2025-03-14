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
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	econtext "vitess.io/vitess/go/vt/vtgate/executorcontext"
)

func TestScatterStatsWithNoScatterQuery(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)

	session := econtext.NewSafeSession(&vtgatepb.Session{TargetString: "@primary"})

	_, err := executorExecSession(ctx, executor, session, "select * from main1", nil)
	require.NoError(t, err)

	result, err := executor.gatherScatterStats()
	require.NoError(t, err)
	require.Equal(t, 0, len(result.Items))
}

func TestScatterStatsWithSingleScatterQuery(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)
	session := econtext.NewSafeSession(&vtgatepb.Session{TargetString: "@primary"})

	_, err := executorExecSession(ctx, executor, session, "select * from user", nil)
	require.NoError(t, err)

	result, err := executor.gatherScatterStats()
	require.NoError(t, err)
	require.Equal(t, 1, len(result.Items))
}

func TestScatterStatsHttpWriting(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)
	session := econtext.NewSafeSession(&vtgatepb.Session{TargetString: "@primary"})

	_, err := executorExecSession(ctx, executor, session, "select * from user", nil)
	require.NoError(t, err)

	_, err = executorExecSession(ctx, executor, session, "select * from user where Id = 15", nil)
	require.NoError(t, err)

	_, err = executorExecSession(ctx, executor, session, "select * from user where Id > 15", nil)
	require.NoError(t, err)

	query4 := "select * from user as u1 join  user as u2 on u1.Id = u2.Id"
	_, err = executorExecSession(ctx, executor, session, query4, nil)
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	recorder := httptest.NewRecorder()
	executor.WriteScatterStats(recorder)

	// Here we are checking that the template was executed correctly.
	// If it wasn't, instead of html, we'll get an error message
	require.Contains(t, recorder.Body.String(), "select * from `user` as u1 join `user` as u2 on u1.Id = u2.Id")
	require.NoError(t, err)
}
