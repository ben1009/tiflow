// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package verification

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"go.uber.org/zap"
)

type checkSumChecker interface {
	getCheckSum(ctx context.Context, f *filter.Filter) (map[string]string, error)
	getAllTables(ctx context.Context, f *filter.Filter) ([]string, error)
	getColumns(ctx context.Context, tableName string) ([]columnInfo, error)
	doChecksum(ctx context.Context, columns []columnInfo, tableName string) (string, error)
}

type checker struct {
	db     *sql.DB
	dbName string
}

// newChecker return a checker instance, which can be used to get checker checksum
func newChecker(db *sql.DB, dbName string) *checker {
	return &checker{
		db:     db,
		dbName: dbName,
	}
}

// GetCheckSum get checker checksum for each table in the given database
func (c *checker) getCheckSum(ctx context.Context, f *filter.Filter) (map[string]string, error) {
	tables, err := c.getAllTables(ctx, f)
	if err != nil {
		return nil, err
	}

	result := make(map[string]string)
	for _, table := range tables {
		columns, err := c.getColumns(ctx, table)
		if err != nil {
			return result, err
		}
		checkSum, err := c.doChecksum(ctx, columns, table)
		if err != nil {
			return result, err
		}
		result[table] = checkSum
	}

	return result, nil
}

func (c *checker) getAllTables(ctx context.Context, f *filter.Filter) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrMySQLQueryError, err)
	}
	defer func() {
		if err = rows.Close(); err != nil {
			log.Error("getAllTables close rows failed", zap.Error(err))
		}
	}()

	tables := []string{}
	for rows.Next() {
		var t string
		if err = rows.Scan(&t); err != nil {
			return tables, cerror.WrapError(cerror.ErrMySQLQueryError, err)
		}
		if f.ShouldIgnoreTable(c.dbName, t) {
			continue
		}
		tables = append(tables, t)
	}

	return tables, nil
}

type columnInfo struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default *string
	Extra   string
}

func (c *checker) getColumns(ctx context.Context, tableName string) ([]columnInfo, error) {
	rows, err := c.db.QueryContext(ctx, fmt.Sprintf("SHOW COLUMNS FROM %s", tableName))
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrMySQLQueryError, err)
	}
	defer func() {
		if err = rows.Close(); err != nil {
			log.Error("getColumns close rows failed", zap.Error(err))
		}
	}()

	result := []columnInfo{}
	for rows.Next() {
		var t columnInfo
		if err = rows.Scan(&t.Field, &t.Type, &t.Null, &t.Key, &t.Default, &t.Extra); err != nil {
			return result, cerror.WrapError(cerror.ErrMySQLQueryError, err)
		}
		result = append(result, t)
	}

	return result, nil
}

func (c *checker) doChecksum(ctx context.Context, columns []columnInfo, tableName string) (string, error) {
	var columnNames, isNull []string
	for _, item := range columns {
		columnNames = append(columnNames, item.Field)

		t := fmt.Sprintf("ISNULL(%s)", item.Field)
		isNull = append(isNull, t)
	}

	a := strings.Join(columnNames, ",")
	b := strings.Join(isNull, ",")

	// ref: https://www.percona.com/doc/percona-toolkit/LATEST/pt-table-checksum.html
	// TODO: hash function as a option
	concat := fmt.Sprintf("CONCAT_WS(',', %s, %s)", a, b)
	query := fmt.Sprintf("SELECT BIT_XOR(CAST(checker(%s) AS UNSIGNED)) AS checksum FROM %s", concat, tableName)

	log.Debug("do checkSum", zap.String("table", tableName), zap.String("query", query))
	var checkSum string
	err := c.db.QueryRowContext(ctx, query).Scan(&checkSum)
	return checkSum, cerror.WrapError(cerror.ErrMySQLQueryError, err)
}

// TODO: use ADMIN CHECKSUM TABLE for tidb if needed
func compareCheckSum(ctx context.Context, upstreamChecker, downstreamChecker checkSumChecker, f *filter.Filter) (bool, error) {
	sourceCheckSum, err := upstreamChecker.getCheckSum(ctx, f)
	if err != nil {
		return false, err
	}

	sinkCheckSum, err := downstreamChecker.getCheckSum(ctx, f)
	if err != nil {
		return false, err
	}

	if len(sourceCheckSum) != len(sinkCheckSum) {
		log.Warn("source and sink have different checker size", zap.Reflect("source", sourceCheckSum), zap.Reflect("sink", sinkCheckSum))
	}

	for k, v := range sourceCheckSum {
		target, ok := sinkCheckSum[k]
		if !ok {
			log.Warn("cannot find checker at sink, it may eligible to replicate", zap.String("tableName", k), zap.String("source checker", v))
			continue
		}
		if v != target {
			log.Error("checker mismatch", zap.String("source", v), zap.String("sink", target))
			return false, nil
		}
	}
	return true, nil
}
