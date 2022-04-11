// Copyright 2021 PingCAP, Inc.
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

package db

//go:generate mockery --name=DB --inpackage
// DB is an interface of a leveldb-like database.
type DB interface {
	// Iterator iterates over a DB's key/value pairs in key order, in the range [lowerBound,upperBound)
	Iterator(lowerBound, upperBound []byte) Iterator
	// Batch returns a new empty batch, the cap param is not used in pebble
	Batch(cap int) Batch
	// DeleteRange deletes all the keys (and values) in the range [start,end)
	DeleteRange(start, end []byte) error
	Compact(start, end []byte) error
	Close() error
	CollectMetrics(id int)
}

//go:generate mockery --name=Batch --inpackage
// A Batch is a sequence of Puts and Deletes that Commit to DB.
type Batch interface {
	Put(key, value []byte)
	Delete(key []byte)
	Commit() error
	Count() uint32
	Repr() []byte
	Reset()
}

// Iterator is an interface of an iterator of a DB.
type Iterator interface {
	Valid() bool
	Seek([]byte) bool
	Next() bool
	Key() []byte
	Value() []byte
	Error() error
	Release() error
}

// SystemTag defines db is used by which system to separate the data from different systems stored in db,
// e.g. Sorter
type SystemTag int

//go:generate stringer -type=SystemTag
const (
	Sorter SystemTag = iota
	Verification
)
