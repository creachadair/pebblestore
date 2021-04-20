// Copyright 2021 Michael J. Fromberger. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package pebblestore implements the blob.Store interface using Pebble.
package pebblestore

import (
	"context"

	"github.com/cockroachdb/pebble"
	"github.com/creachadair/ffs/blob"
)

// Store implements the blob.Store interface using a bbolt database.
type Store struct {
	db *pebble.DB
}

// Opener constructs a filestore from an address comprising a path, for use
// with the store package. If addr has the form name@path, the name is used as
// the bucket label.
func Opener(_ context.Context, addr string) (blob.Store, error) {
	panic("ok")
}

// Open creates a Store by opening the bbolt database specified by opts.
func Open(path string, opts *Options) (*Store, error) {
	panic("ok")
}

// Options provides options for opening a bbolt database.
type Options struct {
}

// Close implements the io.Closer interface. It closes the underlying database
// instance and reports its result.
func (s *Store) Close() error { return s.db.Close() }

// Get implements part of blob.Store.
func (s *Store) Get(_ context.Context, key string) (data []byte, err error) {
	panic("ok")
}

// Put implements part of blob.Store. A successful Put linearizes to the point
// at which the rename of the write temporary succeeds; a Put that fails due to
// an existing key linearizes to the point when the key path stat succeeds.
func (s *Store) Put(_ context.Context, opts blob.PutOptions) error {
	panic("ok")
}

// Size implements part of blob.Store.
func (s *Store) Size(_ context.Context, key string) (size int64, err error) {
	panic("ok")
}

// Delete implements part of blob.Store.
func (s *Store) Delete(_ context.Context, key string) error {
	panic("ok")
}

// List implements part of blob.Store.
func (s *Store) List(_ context.Context, start string, f func(string) error) error {
	panic("ok")
}

// Len implements part of blob.Store.
func (s *Store) Len(ctx context.Context) (int64, error) {
	panic("ok")
}
