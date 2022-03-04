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
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/db"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

type Module int

const (
	Puller Module = iota
	Sorter
	Cyclic
	Sink
)

const (
	// Double batch capacity to avoid memory reallocation.
	writeBatchCapFactor = 2
	// Write batch size should be larger than block size to save CPU.
	writeBatchSizeFactor = 16
)

type ModuleVerifier interface {
	// SentTrackData sent the track data to verification, the data should send according to the order if any. data is FIFO guaranteed, in the same changefeed, same module
	SentTrackData(ctx context.Context, module Module, data []TrackData)
	// Verify run the module level consistency check base on the time range (startTs, endTs]
	Verify(ctx context.Context, startTs, endTs string) error
	// GC clan the used trackData
	GC() error
	// Close clean related resource
	Close() error
}

type ModuleVerification struct {
	cfg *ModuleVerificationConfig
	db  db.DB
	// write-only
	wb           db.Batch
	deleteCount  uint64
	nextDeleteTs time.Time
}

type ModuleVerificationConfig struct {
	ChangeFeedID        string
	DBConfig            *config.DBConfig
	MaxMemoryPercentage int
}

var (
	verifications = map[string]*ModuleVerification{}
	initLock      sync.Mutex
)

// NewModuleVerification return the module level verification per changefeed
func NewModuleVerification(ctx context.Context, cfg *ModuleVerificationConfig) (*ModuleVerification, error) {
	if cfg == nil {
		return nil, cerror.WrapError(cerror.ErrVerificationConfigInvalid, errors.New("ModuleVerificationConfig can not be nil"))
	}

	initLock.Lock()
	defer initLock.Unlock()

	if v, ok := verifications[cfg.ChangeFeedID]; ok {
		return v, nil
	}

	totalMemory, err := memory.MemTotal()
	if err != nil {
		return nil, err
	}
	if cfg.MaxMemoryPercentage == 0 {
		cfg.MaxMemoryPercentage = 20
	}
	memPercentage := float64(cfg.MaxMemoryPercentage) / 100
	memInByte := float64(totalMemory) * memPercentage

	dir := filepath.Join(config.GetDefaultServerConfig().DataDir, config.DefaultVerificationDir, cfg.ChangeFeedID)
	if cfg.DBConfig == nil {
		cfg.DBConfig = config.GetDefaultServerConfig().Debug.DB
	}
	// TODO: meaningful id value?
	pebble, err := db.OpenPebble(ctx, 0, dir, int(memInByte), cfg.DBConfig)
	if err != nil {
		return nil, err
	}

	wbSize := cfg.DBConfig.BlockSize * writeBatchSizeFactor
	c := wbSize * writeBatchCapFactor
	wb := pebble.Batch(c)

	m := &ModuleVerification{
		db: pebble,
		wb: wb,
	}
	verifications[cfg.ChangeFeedID] = m

	return m, nil
}

func checkOrder(module Module) bool {
	if module == Sorter || module == Sink {
		return true
	}
	return false
}

var (
	preTsList = map[string]uint64{}
	preLock   sync.RWMutex
)

type TrackData struct {
	TrackID  string
	CommitTs uint64
}

// SentTrackData implement SentTrackData api
func (m *ModuleVerification) SentTrackData(ctx context.Context, module Module, data []TrackData) {
	select {
	case <-ctx.Done():
		log.Info("SentTrackData ctx cancel", zap.Error(errors.Trace(ctx.Err())))
		return
	default:
	}

	var preTs uint64 = 0
	var tsKey string
	if checkOrder(module) {
		preLock.RLock()
		tsKey = m.generatePreTsKey(module)
		if v, ok := preTsList[tsKey]; ok {
			preTs = v
		}
		preLock.RUnlock()
	}

	for _, datum := range data {
		if checkOrder(module) {
			if datum.CommitTs < preTs {
				log.Error("module level verify fail, the commitTs of the data is less than previous data",
					zap.Int("module", int(module)),
					zap.Any("trackData", data),
					zap.Uint64("preTs", preTs))
			}
			preTs = datum.CommitTs
		}

		key := fmt.Sprintf("%d_%d_%s", datum.CommitTs, module, datum.TrackID)
		m.wb.Put([]byte(key), nil)
		size := m.cfg.DBConfig.BlockSize * writeBatchSizeFactor
		if len(m.wb.Repr()) >= size {
			err := m.wb.Commit()
			if err != nil {
				log.Error("SentTrackData commit to db fail", zap.Error(cerror.WrapError(cerror.ErrPebbleDBError, err)))
			}
		}

		if checkOrder(module) {
			preLock.Lock()
			preTsList[tsKey] = preTs
			preLock.Unlock()
		}
	}
}

func (m *ModuleVerification) generatePreTsKey(module Module) string {
	return fmt.Sprintf("%s_%d", m.cfg.ChangeFeedID, module)
}

// Verify TODO: cyclic enable
func (m *ModuleVerification) Verify(ctx context.Context, startTs, endTs string) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	ret := m.shouldExist(Puller, Sink, startTs, endTs)
	if ret {
		log.Info("module level check pass")
		return nil
	}

	ret = m.shouldExist(Puller, Sorter, startTs, endTs)
	if ret {
		log.Error("model level check fail", zap.String("module", "sink"))
	} else {
		log.Error("model level check fail", zap.String("module", "sorter"))
	}

	m.deleteCount++
	err := m.db.DeleteRange([]byte(startTs), []byte(endTs))
	if err != nil {
		return cerror.WrapError(cerror.ErrPebbleDBError, err)
	}

	return nil
}

func (m *ModuleVerification) GC() error {
	now := time.Now()
	if m.deleteCount < uint64(m.cfg.DBConfig.CompactionDeletionThreshold) && m.nextDeleteTs.Before(now) {
		return nil
	}

	start, end := []byte{0x0}, bytes.Repeat([]byte{0xff}, 128)
	err := m.db.Compact(start, end)
	if err != nil {
		return cerror.WrapError(cerror.ErrPebbleDBError, err)
	}
	m.deleteCount = 0
	interval := time.Duration(m.cfg.DBConfig.CompactionPeriod) * time.Second
	m.nextDeleteTs = now.Add(interval)
	return nil
}

func (m *ModuleVerification) shouldExist(m1, m2 Module, lower, upper string) bool {
	l1 := fmt.Sprintf("%s_%d", lower, m1)
	u1 := fmt.Sprintf("%s_%d", upper, m1)
	iter1 := m.db.Iterator([]byte(l1), []byte(u1))
	defer iter1.Release()
	iter1.Seek([]byte(l1))

	l2 := fmt.Sprintf("%s_%d", lower, m2)
	u2 := fmt.Sprintf("%s_%d", upper, m2)
	iter2 := m.db.Iterator([]byte(l2), []byte(u2))
	defer iter2.Release()
	iter2.Seek([]byte(l2))

	for iter1.Valid() || iter2.Valid() {
		if !iter1.Valid() {
			return true
		}
		if !iter2.Valid() {
			return false
		}
		ret := bytes.Compare(iter1.Key(), iter2.Key())
		if ret == 0 {
			iter1.Next()
			iter2.Next()
		} else if ret > 0 {
			iter2.Next()
		} else {
			return false
		}
	}
	return true
}

func (m *ModuleVerification) Close() error {
	return cerror.WrapError(cerror.ErrPebbleDBError, m.db.Close())
}
