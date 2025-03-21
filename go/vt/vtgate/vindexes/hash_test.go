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

package vindexes

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var hashTest SingleColumn

func init() {
	hv, err := CreateVindex("hash", "nn", map[string]string{})
	unknownParams := hv.(ParamValidating).UnknownParams()
	if len(unknownParams) > 0 {
		panic("hash test init: expected 0 unknown params")
	}
	if err != nil {
		panic(err)
	}
	hashTest = hv.(SingleColumn)
}

func hashCreateVindexTestCase(
	testName string,
	vindexParams map[string]string,
	expectErr error,
	expectUnknownParams []string,
) createVindexTestCase {
	return createVindexTestCase{
		testName: testName,

		vindexType:   "hash",
		vindexName:   "hash",
		vindexParams: vindexParams,

		expectCost:          1,
		expectErr:           expectErr,
		expectIsUnique:      true,
		expectNeedsVCursor:  false,
		expectString:        "hash",
		expectUnknownParams: expectUnknownParams,
	}
}

func TestHashCreateVindex(t *testing.T) {
	cases := []createVindexTestCase{
		hashCreateVindexTestCase(
			"no params",
			nil,
			nil,
			nil,
		),
		hashCreateVindexTestCase(
			"empty params",
			map[string]string{},
			nil,
			nil,
		),
		hashCreateVindexTestCase(
			"unknown params",
			map[string]string{"hello": "world"},
			nil,
			[]string{"hello"},
		),
	}

	testCreateVindexes(t, cases)
}

func TestHashMap(t *testing.T) {
	got, err := hashTest.Map(context.Background(), nil, []sqltypes.Value{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
		sqltypes.NewInt64(3),
		sqltypes.NULL,
		sqltypes.NewInt64(4),
		sqltypes.NewInt64(5),
		sqltypes.NewInt64(6),
		sqltypes.NewInt64(0),
		sqltypes.NewInt64(-1),
		sqltypes.NewUint64(18446744073709551615), // 2^64 - 1
		sqltypes.NewInt64(9223372036854775807),   // 2^63 - 1
		sqltypes.NewUint64(9223372036854775807),  // 2^63 - 1
		sqltypes.NewInt64(-9223372036854775808),  // - 2^63
	})
	require.NoError(t, err)
	want := []key.ShardDestination{
		key.DestinationKeyspaceID([]byte("\x16k@\xb4J\xbaK\xd6")),
		key.DestinationKeyspaceID([]byte("\x06\xe7\xea\"Βp\x8f")),
		key.DestinationKeyspaceID([]byte("N\xb1\x90ɢ\xfa\x16\x9c")),
		key.DestinationNone{},
		key.DestinationKeyspaceID([]byte("\xd2\xfd\x88g\xd5\r-\xfe")),
		key.DestinationKeyspaceID([]byte("p\xbb\x02<\x81\f\xa8z")),
		key.DestinationKeyspaceID([]byte("\xf0\x98H\n\xc4ľq")),
		key.DestinationKeyspaceID([]byte("\x8c\xa6M\xe9\xc1\xb1#\xa7")),
		key.DestinationKeyspaceID([]byte("5UP\xb2\x15\x0e$Q")),
		key.DestinationKeyspaceID([]byte("5UP\xb2\x15\x0e$Q")),
		key.DestinationKeyspaceID([]byte("\xf7}H\xaaݡ\xf1\xbb")),
		key.DestinationKeyspaceID([]byte("\xf7}H\xaaݡ\xf1\xbb")),
		key.DestinationKeyspaceID([]byte("\x95\xf8\xa5\xe5\xdd1\xd9\x00")),
	}
	if !reflect.DeepEqual(got, want) {
		for i, v := range got {
			if v.String() != want[i].String() {
				t.Errorf("Map() %d: %#v, want %#v", i, v, want[i])
			}
		}
	}
}

func TestHashVerify(t *testing.T) {
	ids := []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}
	ksids := [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6"), []byte("\x16k@\xb4J\xbaK\xd6")}
	got, err := hashTest.Verify(context.Background(), nil, ids, ksids)
	if err != nil {
		t.Fatal(err)
	}
	want := []bool{true, false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("binaryMD5.Verify: %v, want %v", got, want)
	}

	// Failure test
	_, err = hashTest.Verify(context.Background(), nil, []sqltypes.Value{sqltypes.NewVarBinary("aa")}, [][]byte{nil})
	require.EqualError(t, err, "cannot parse uint64 from \"aa\"")
}

func TestHashReverseMap(t *testing.T) {
	got, err := hashTest.(Reversible).ReverseMap(nil, [][]byte{
		[]byte("\x16k@\xb4J\xbaK\xd6"),
		[]byte("\x06\xe7\xea\"Βp\x8f"),
		[]byte("N\xb1\x90ɢ\xfa\x16\x9c"),
		[]byte("\xd2\xfd\x88g\xd5\r-\xfe"),
		[]byte("p\xbb\x02<\x81\f\xa8z"),
		[]byte("\xf0\x98H\n\xc4ľq"),
		[]byte("\x8c\xa6M\xe9\xc1\xb1#\xa7"),
		[]byte("5UP\xb2\x15\x0e$Q"),
		[]byte("5UP\xb2\x15\x0e$Q"),
		[]byte("\xf7}H\xaaݡ\xf1\xbb"),
		[]byte("\xf7}H\xaaݡ\xf1\xbb"),
		[]byte("\x95\xf8\xa5\xe5\xdd1\xd9\x00"),
	})
	require.NoError(t, err)
	neg1 := int64(-1)
	negmax := int64(-9223372036854775808)
	want := []sqltypes.Value{
		sqltypes.NewUint64(uint64(1)),
		sqltypes.NewUint64(2),
		sqltypes.NewUint64(3),
		sqltypes.NewUint64(4),
		sqltypes.NewUint64(5),
		sqltypes.NewUint64(6),
		sqltypes.NewUint64(0),
		sqltypes.NewUint64(uint64(neg1)),
		sqltypes.NewUint64(18446744073709551615), // 2^64 - 1
		sqltypes.NewUint64(9223372036854775807),  // 2^63 - 1
		sqltypes.NewUint64(9223372036854775807),  // 2^63 - 1
		sqltypes.NewUint64(uint64(negmax)),       // - 2^63
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReverseMap(): %v, want %v", got, want)
	}
}

func TestHashReverseMapNeg(t *testing.T) {
	_, err := hashTest.(Reversible).ReverseMap(nil, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6\x16k@\xb4J\xbaK\xd6")})
	want := "invalid keyspace id: 166b40b44aba4bd6166b40b44aba4bd6"
	if err.Error() != want {
		t.Error(err)
	}
}
