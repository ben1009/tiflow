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
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type Verifier interface {
	Verify(ctx context.Context) (map[string]*TimeRange, error)
	Close() error
}

type Config struct {
	CheckIntervalInSec time.Duration
	ResourceLimitation string
	UpstreamDSN        string
	DownStreamDSN      string
	Filter             *filter.Filter
	DataBaseName       string
	TableName          string
	StartTs            uint64
	ChangefeedID       string
}

type TiDBVerification struct {
	config            *Config
	upstreamChecker   *checker
	downstreamChecker *checker
	running           atomic.Bool
}

const (
	defaultCheckInterval = 60 * time.Second
)

const (
	unchecked = iota
	checkPass
	checkFail
)

func NewVerification(ctx context.Context, config *Config) error {
	upstreamDB, err := openDB(ctx, config.UpstreamDSN)
	if err != nil {
		return err
	}
	downstreamDB, err := openDB(ctx, config.DownStreamDSN)
	if err != nil {
		return err
	}

	if config.CheckIntervalInSec == 0 {
		config.CheckIntervalInSec = defaultCheckInterval
	}
	v := &TiDBVerification{
		config:            config,
		upstreamChecker:   newChecker(upstreamDB),
		downstreamChecker: newChecker(downstreamDB),
	}
	go v.runVerify(ctx)

	return nil
}

type TimeRange struct {
	StatTs string
	EndTs  string
}

func (v *TiDBVerification) runVerify(ctx context.Context) {
	ticker := time.NewTicker(v.config.CheckIntervalInSec)
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
		_, _, err := v.Verify(ctx)
		if err != nil {
			log.Warn("runVerify fail", zap.Error(err))
		}

		// TODO: module level check

	}
}

func (v *TiDBVerification) Verify(ctx context.Context) (string, string, error) {
	if v.running.Load() {
		return "", "", nil
	}

	v.running.Store(true)
	defer v.running.Store(false)

	ts, err := v.getTS(ctx)
	if err != nil {
		return "", "", err
	}
	if ts.result != unchecked {
		return "", "", nil
	}

	p, err := strconv.Atoi(ts.primaryTs)
	if err != nil {
		return "", "", err
	}
	if uint64(p) > v.config.StartTs {
		v.config.StartTs = uint64(p)
	}

	startTs, endTs := "", ""
	for ts.result == unchecked {
		ret, err := v.checkConsistency(ctx, ts)
		if err != nil {
			return "", "", err
		}

		checkRet := checkPass
		if !ret {
			checkRet = checkFail
		}
		err = v.updateCheckResult(ctx, ts, checkRet)
		if err != nil {
			return "", "", err
		}

		// if pass no need to run module check, if run from previous set startTs
		if checkRet == checkPass {
			if endTs != "" {
				startTs = ts.primaryTs
			}
			break
		}

		preTs, err := v.getPreviousTS(ctx, ts.cf, ts.primaryTs)
		if err != nil {
			if sql.ErrNoRows == errors.Cause(err) {
				endTs = ts.primaryTs
				break
			}
			return "", "", err
		}

		// if previous check pass run module check, if fail means already run module check last time, skip, if unchecked, run e2e check against previous
		if preTs.result == checkPass {
			startTs = preTs.primaryTs
			endTs = ts.primaryTs
		}
		if preTs.result == unchecked {
			endTs = ts.primaryTs
		}
		ts = preTs
	}

	return startTs, endTs, nil
}

func (v *TiDBVerification) checkConsistency(ctx context.Context, t tsPair) (bool, error) {
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

func (v *TiDBVerification) updateCheckResult(ctx context.Context, t tsPair, checkRet int) error {
	tx, err := v.upstreamChecker.db.BeginTx(ctx, nil)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
	}

	_, err = tx.ExecContext(ctx, fmt.Sprintf("update %s.%s set result=%d where primary_ts=%s and secondary_ts=%s and cf=%s", v.config.DataBaseName, v.config.TableName, checkRet, t.primaryTs, t.secondaryTs, t.cf))
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

func (v *TiDBVerification) getPreviousTS(ctx context.Context, cf, pts string) (tsPair, error) {
	var t tsPair
	row := v.upstreamChecker.db.QueryRowContext(ctx, fmt.Sprintf("select cf, promary_ts, secondary_ts, result from %s.%s where cf=%s and primary_ts<%s order by primary_ts desc limit 1", v.config.DataBaseName, v.config.TableName, cf, pts))
	if row.Err() != nil {
		return t, cerror.WrapError(cerror.ErrMySQLQueryError, row.Err())
	}

	if err := row.Scan(&t.cf, &t.primaryTs, &t.secondaryTs, &t.result); err != nil {
		return t, cerror.WrapError(cerror.ErrMySQLQueryError, err)
	}
	return t, nil
}

func (v *TiDBVerification) getTS(ctx context.Context) (tsPair, error) {
	var ts tsPair
	row := v.upstreamChecker.db.QueryRowContext(ctx, fmt.Sprintf("select max(primary_ts), cf from %s.%s where primary_ts>= %s and cf=%s", v.config.DataBaseName, v.config.TableName, strconv.Itoa(int(v.config.StartTs)), v.config.ChangefeedID))
	if row.Err() != nil {
		return ts, cerror.WrapError(cerror.ErrMySQLQueryError, row.Err())
	}
	if err := row.Scan(&ts.primaryTs, &ts.cf); err != nil {
		return ts, cerror.WrapError(cerror.ErrMySQLQueryError, err)
	}

	row = v.upstreamChecker.db.QueryRowContext(ctx, fmt.Sprintf("select cf, promary_ts, secondary_ts, result from %s.%s where cf=%s and primary_ts=%s", v.config.DataBaseName, v.config.TableName, ts.cf, ts.primaryTs))
	if row.Err() != nil {
		return ts, cerror.WrapError(cerror.ErrMySQLQueryError, row.Err())
	}
	if err := row.Scan(&ts.cf, &ts.primaryTs, &ts.secondaryTs, &ts.result); err != nil {
		return ts, cerror.WrapError(cerror.ErrMySQLQueryError, err)
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
