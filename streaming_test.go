package duckdb

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStreamingResultsReadsEveryRow(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	rows, err := db.QueryContext(WithStreamingResults(context.Background()), `SELECT i FROM range(1000000) t(i)`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, rows)
	var n, sum, v int64
	for rows.Next() {
		require.NoError(t, rows.Scan(&v))
		n++
		sum += v
	}
	require.NoError(t, rows.Err())
	require.Equal(t, int64(1000000), n)
	require.Equal(t, int64(499999500000), sum)
}

// A streaming query that fails part-way through must report the failure, not end early.
func TestStreamingResultsReportsAFailurePartWay(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	rows, err := db.QueryContext(WithStreamingResults(context.Background()),
		`SELECT CASE WHEN i = 600000 THEN error('boom') ELSE i END FROM range(1000000) t(i)`)
	require.NoError(t, err, "the failure is past the first chunk, so it surfaces while fetching")
	defer closeRowsWrapper(t, rows)
	var n, v int64
	for rows.Next() {
		require.NoError(t, rows.Scan(&v))
		n++
	}
	require.ErrorContains(t, rows.Err(), "boom")
	require.Less(t, n, int64(600000))
}

func TestQueryWithoutStreamingReportsAFailureUpFront(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.QueryContext(context.Background(),
		`SELECT CASE WHEN i = 600000 THEN error('boom') ELSE i END FROM range(1000000) t(i)`)
	require.ErrorContains(t, err, "boom")
}

type streamingFetchProbe struct {
	info         TypeInfo
	fetchStarted chan struct{}
	stop         atomic.Bool
}

func (p *streamingFetchProbe) Config() ScalarFuncConfig {
	return ScalarFuncConfig{InputTypeInfos: []TypeInfo{p.info}, ResultTypeInfo: p.info}
}

func (p *streamingFetchProbe) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{RowExecutor: func(values []driver.Value) (any, error) {
		i := values[0].(int64)
		if i == 1_000_000 {
			close(p.fetchStarted)
		}
		if p.stop.Load() {
			return nil, errors.New("test stopped the scan")
		}
		if i < 100_000 {
			return i, nil
		}
		return int64(-1), nil
	}}
}

func TestStreamingCancellationDuringFetch(t *testing.T) {
	for _, mode := range []string{"QueryContext", "QueryBound"} {
		t.Run(mode, func(t *testing.T) {
			db := openDbWrapper(t, "")
			defer closeDbWrapper(t, db)
			ctx := context.Background()
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer func() { require.NoError(t, conn.Close()) }()
			_, err = conn.ExecContext(ctx, "SET threads = 1")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "SET streaming_buffer_size = '64 KiB'")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "CREATE TEMP TABLE streaming_session AS SELECT 42 AS n")
			require.NoError(t, err)

			info, err := NewTypeInfo(TYPE_BIGINT)
			require.NoError(t, err)
			probe := &streamingFetchProbe{info: info, fetchStarted: make(chan struct{})}
			require.NoError(t, RegisterScalarUDF(conn, "streaming_fetch_probe", probe))

			queryCtx, cancel := context.WithCancel(WithStreamingResults(ctx))
			defer cancel()
			delivered := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- conn.Raw(func(raw any) error {
					const query = "SELECT i FROM range(1000000000) t(i) WHERE streaming_fetch_probe(i) >= 0"
					native := raw.(*Conn)
					var result driver.Rows
					var queryErr error
					if mode == "QueryBound" {
						prepared, err := native.PrepareContext(ctx, query)
						if err != nil {
							return err
						}
						stmt := prepared.(*Stmt)
						defer func() { _ = stmt.Close() }()
						if err := stmt.BindWithCtx(ctx, nil); err != nil {
							return err
						}
						result, queryErr = stmt.QueryBound(queryCtx)
					} else {
						result, queryErr = native.QueryContext(queryCtx, query, nil)
					}

					if queryErr != nil {
						return queryErr
					}
					defer func() { _ = result.Close() }()
					dst := make([]driver.Value, 1)
					if err := result.Next(dst); err != nil {
						return err
					}
					close(delivered)
					for {
						if err := result.Next(dst); err != nil {
							return err
						}
					}
				})
			}()

			// Release the scan if an assertion fails, then wait for Raw to release the connection.
			finished := false
			defer func() {
				probe.stop.Store(true)
				if !finished {
					t.Logf("query ended during test cleanup: %v", <-done)
				}
			}()
			select {
			case <-delivered:

			case err := <-done:
				finished = true
				t.Fatalf("query ended before delivering rows: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("query did not deliver its first row")
			}
			select {
			case <-probe.fetchStarted:
			case err := <-done:
				finished = true
				t.Fatalf("query ended before entering the later fetch: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("later native fetch did not reach the probe")
			}
			cancel()
			select {
			case err := <-done:
				finished = true
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(3 * time.Second):
				t.Fatal("cancellation did not interrupt the native fetch")
			}

			var n int
			require.NoError(t, conn.QueryRowContext(ctx, "SELECT n FROM streaming_session").Scan(&n))
			require.Equal(t, 42, n)
		})
	}
}

func TestStreamingEarlyCloseAndExecCounts(t *testing.T) {
	db := openDbWrapper(t, "")
	defer closeDbWrapper(t, db)
	ctx, cancel := context.WithCancel(WithStreamingResults(context.Background()))
	defer cancel()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	_, err = conn.ExecContext(ctx, "CREATE TEMP TABLE streaming_counts (n BIGINT)")
	require.NoError(t, err)
	result, err := conn.ExecContext(ctx, "INSERT INTO streaming_counts SELECT i FROM range(100000) t(i)")
	require.NoError(t, err)
	n, err := result.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 100000, n)

	rows, err := conn.QueryContext(ctx, "SELECT n FROM streaming_counts")
	require.NoError(t, err)
	defer closeRowsWrapper(t, rows)
	require.True(t, rows.Next())
	require.NoError(t, rows.Close())
	cancel()
	require.NoError(t, conn.QueryRowContext(context.Background(), "SELECT count(*) FROM streaming_counts").Scan(&n))
	require.EqualValues(t, 100000, n)
}
