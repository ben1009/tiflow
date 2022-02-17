//  Copyright 2022 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package verification

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/sink"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"go.uber.org/zap"
)

type Verifier interface {
	Verify(ctx context.Context) (map[string]*TimeRange, error)
	Close() error
}

type Config struct {
	CheckIntervalInSec int
	ResourceLimitation string
	UpstreamDSN        string //with dbname ?
	DownStreamDSN      string
	DBName             string
	Filter             *filter.Filter
}

type TiDBVerification struct {
	config            *Config
	upstreamChecker   *checker
	downstreamChecker *checker
}

const (
	defaultCheckIntervalInSec = 60
)

const (
	unchecked = iota
	checkPass
	checkFail
)

func NewVerification(ctx context.Context, config *Config) (*TiDBVerification, error) {
	upstreamDB, err := openDB(ctx, config.UpstreamDSN)
	if err != nil {
		return nil, err
	}
	downstreamDB, err := openDB(ctx, config.DownStreamDSN)
	if err != nil {
		return nil, err
	}

	if config.CheckIntervalInSec == 0 {
		config.CheckIntervalInSec = defaultCheckIntervalInSec
	}
	v := &TiDBVerification{
		config:            config,
		upstreamChecker:   newChecker(upstreamDB, config.DBName),
		downstreamChecker: newChecker(downstreamDB, config.DBName),
	}
	go v.runVerify(ctx)

	return v, nil
}

type TimeRange struct {
	StatTs string
	EndTs  string
}

func (v *TiDBVerification) runVerify(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(v.config.CheckIntervalInSec) * time.Second)
	defer ticker.Stop()

	select {
	case <-ctx.Done():
		err := v.Close()
		if err != nil {
			log.Error("runVerify Close fail", zap.Error(err))
		}
	case <-ticker.C:
		// TODO:
		// resource limitation cancel https://www.percona.com/doc/percona-toolkit/LATEST/pt-table-checksum.html
		_, err := v.Verify(ctx)
		if err != nil {
			log.Error("runVerify fail", zap.Error(err))
		}

		// TODO: module level check

	}
}

func (v *TiDBVerification) Verify(ctx context.Context) (map[string]*TimeRange, error) {
	tsList, err := v.getTS(ctx)
	if err != nil {
		return nil, err
	}

	rets := map[string]*TimeRange{}
	for _, t := range tsList {
		t1 := t
		for t1.result == unchecked {
			ret, err := v.checkConsistency(ctx, t1)
			if err != nil {
				return nil, err
			}

			checkRet := checkPass
			if !ret {
				checkRet = checkFail
			}
			err = v.updateCheckResult(ctx, t1, checkRet)
			if err != nil {
				return nil, err
			}

			// if pass no need to run module check, if run from previous set startTs
			if checkRet == checkPass {
				if v, ok := rets[t1.cf]; ok {
					v.StatTs = t1.primaryTs
				}
				break
			}

			preTS, err := v.getPreviousTS(ctx, t1.cf, t1.primaryTs)
			if err != nil {
				return nil, err
			}

			if preTS == nil {
				rets[t1.cf] = &TimeRange{EndTs: t1.primaryTs}
				break
			}
			// if previous check pass run module check, if fail means already run module check last time, skip, if unchecked, run e2e check against previous
			if preTS.result == checkPass {
				rets[t1.cf] = &TimeRange{StatTs: preTS.primaryTs, EndTs: t1.primaryTs}
			}
			if preTS.result == unchecked {
				rets[t1.cf] = &TimeRange{EndTs: t1.primaryTs}
			}
			t1 = preTS
		}
	}
	return rets, nil
}

func (v *TiDBVerification) checkConsistency(ctx context.Context, t *tsPair) (bool, error) {
	err := setSnapshot(ctx, v.upstreamChecker.db, t.primaryTs)
	if err != nil {
		return false, err
	}
	err = setSnapshot(ctx, v.downstreamChecker.db, t.secondaryTs)
	if err != nil {
		return false, err
	}
	log.Info("check consistency between upstream and downstream db",
		zap.String("primaryTs", t.primaryTs),
		zap.String("secondaryTs", t.secondaryTs),
		zap.String("changefeed", t.cf))
	return compareCheckSum(ctx, v.upstreamChecker, v.downstreamChecker, v.config.Filter)
}

func (v *TiDBVerification) updateCheckResult(ctx context.Context, t *tsPair, checkRet int) error {
	tx, err := v.upstreamChecker.db.BeginTx(ctx, nil)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
	}

	_, err = tx.ExecContext(ctx, fmt.Sprintf("update %s set result=%d where primary_ts=%s and secondary_ts=%s and cf=%s", sink.SyncpointTableName, checkRet, t.primaryTs, t.secondaryTs, t.cf))
	if err != nil {
		errR := tx.Rollback()
		if errR != nil {
			log.Error("failed to rollback update syncpoint table", zap.Error(cerror.WrapError(cerror.ErrMySQLTxnError, errR)))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
	}
	return cerror.WrapError(cerror.ErrMySQLTxnError, tx.Commit())
}

func openDB(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrMySQLConnectionError, err)
	}
	err = db.PingContext(ctx)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrMySQLConnectionError, err)
	}
	return db, nil
}

func setSnapshot(ctx context.Context, db *sql.DB, ts string) error {
	query := fmt.Sprintf(`set @@tidb_snapshot=%s`, ts)
	_, err := db.ExecContext(ctx, query)
	return cerror.WrapError(cerror.ErrMySQLTxnError, err)
}

type tsPair struct {
	cf          string
	primaryTs   string
	secondaryTs string
	result      int
}

func (v *TiDBVerification) getPreviousTS(ctx context.Context, cf, pts string) (*tsPair, error) {
	var t *tsPair
	row := v.upstreamChecker.db.QueryRowContext(ctx, fmt.Sprintf("select cf, promary_ts, secondary_ts, result from %s where cf=%s and primary_ts<%s order by primary_ts desc limit 1", sink.SyncpointTableName, cf, pts))
	if row.Err() != nil {
		return t, cerror.WrapError(cerror.ErrMySQLQueryError, row.Err())
	}

	if err := row.Scan(&t.cf, &t.primaryTs, &t.secondaryTs, &t.result); err != nil && err != sql.ErrNoRows {
		return t, cerror.WrapError(cerror.ErrMySQLQueryError, err)
	}
	return t, nil
}

func (v *TiDBVerification) getTS(ctx context.Context) ([]*tsPair, error) {
	ts := []*tsPair{}
	rows, err := v.upstreamChecker.db.QueryContext(ctx, fmt.Sprintf("select max(primary_ts), cf from %s group by cf", sink.SyncpointDatabaseName))
	if err != nil {
		return ts, cerror.WrapError(cerror.ErrMySQLQueryError, err)
	}
	defer func() {
		if err = rows.Close(); err != nil {
			log.Error("getTS close rows failed", zap.Error(err))
		}
	}()

	maxTsList := map[string]string{}
	var maxTs, cf string
	for rows.Next() {
		if err = rows.Scan(&maxTs, &cf); err != nil {
			return ts, cerror.WrapError(cerror.ErrMySQLQueryError, err)
		}
		maxTsList[cf] = maxTs
	}

	for cf, pts := range maxTsList {
		row := v.upstreamChecker.db.QueryRowContext(ctx, fmt.Sprintf("select cf, promary_ts, secondary_ts, result from %s where cf=%s and primary_ts=%s", sink.SyncpointTableName, cf, pts))
		if row.Err() != nil {
			return ts, cerror.WrapError(cerror.ErrMySQLQueryError, row.Err())
		}

		var t *tsPair
		if err = row.Scan(&t.cf, &t.primaryTs, &t.secondaryTs, &t.result); err != nil {
			return ts, cerror.WrapError(cerror.ErrMySQLQueryError, err)
		}
		ts = append(ts, t)
	}
	return ts, nil
}

func (v *TiDBVerification) Close() error {
	err := v.upstreamChecker.db.Close()
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLConnectionError, err)
	}
	return cerror.WrapError(cerror.ErrMySQLConnectionError, v.downstreamChecker.db.Close())
}
