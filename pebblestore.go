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
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/creachadair/ffs/blob"
)

// Store implements the blob.Store interface using a Pebble database.
type Store struct {
	db *pebble.DB
	c  io.Closer
}

// Opener constructs a store backed by PebbleDB from an address comprising a
// path, for use with the store package.
func Opener(_ context.Context, addr string) (blob.Store, error) {
	return Open(addr, nil)
}

// Open creates a Store by opening the Pebble database specified by opts.
func Open(path string, opts *Options) (*Store, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &Store{db: db, c: db}, nil
}

// Options provides options for opening a Pebble database.
type Options struct{}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

// Close implements part of the blob.Store interface. It closes the underlying
// database instance and reports its result.
func (s *Store) Close(_ context.Context) error {
	cerr := s.c.Close()
	s.c = nopCloser{}
	return cerr
}

func (s *Store) getRaw(key string) ([]byte, io.Closer, error) {
	val, c, err := s.db.Get([]byte(key))
	if err == pebble.ErrNotFound {
		return nil, nil, blob.KeyNotFound(key)
	} else if err != nil {
		return nil, nil, err
	}
	return val, c, nil
}

// Get implements part of blob.Store.
func (s *Store) Get(_ context.Context, key string) (data []byte, err error) {
	val, c, err := s.getRaw(key)
	if err != nil {
		return nil, err
	}
	data = append([]byte{}, val...)
	c.Close() // required; invalidates val
	return data, nil
}

// Put implements part of blob.Store.
func (s *Store) Put(_ context.Context, opts blob.PutOptions) error {
	key := []byte(opts.Key)
	if !opts.Replace {
		_, c, err := s.db.Get(key)
		if err == nil {
			c.Close()
			return blob.KeyExists(opts.Key)
		}
		// fall through
	}
	return s.db.Set(key, opts.Data, pebble.Sync)
}

// Delete implements part of blob.Store.
func (s *Store) Delete(ctx context.Context, key string) error {
	_, c, err := s.getRaw(key)
	if err != nil {
		return err
	}
	c.Close()
	return s.db.Delete([]byte(key), pebble.Sync)
}

// List implements part of blob.Store.
func (s *Store) List(ctx context.Context, start string, f func(string) error) error {
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte(start)})
	if err != nil {
		return err
	}
	for it.First(); it.Valid(); it.Next() {
		err := f(string(it.Key()))
		if err == blob.ErrStopListing {
			break
		} else if err != nil {
			it.Close()
			return err
		} else if err := ctx.Err(); err != nil {
			it.Close()
			return err
		}
	}
	return it.Close()
}

// Len implements part of blob.Store.
func (s *Store) Len(ctx context.Context) (int64, error) {
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("")})
	if err != nil {
		return 0, err
	}
	var count int64
	for it.First(); it.Valid(); it.Next() {
		count++
	}
	return count, it.Close()
}
