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

// Package pebblestore implements the [blob.StoreCloser] interface on Pebble.
package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/dbkey"
	"github.com/creachadair/ffs/storage/monitor"
)

// Opener constructs a store backed by PebbleDB from an address comprising a
// path, for use with the store package.
func Opener(_ context.Context, addr string) (blob.StoreCloser, error) {
	return Open(addr, nil)
}

// Open creates a [KV] by opening the Pebble database specified by opts.
func Open(path string, opts *Options) (Store, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return Store{}, err
	}
	return Store{M: monitor.New(monitor.Config[*pebble.DB, KV]{
		DB: db,
		NewKV: func(_ context.Context, db *pebble.DB, pfx dbkey.Prefix, _ string) (KV, error) {
			return KV{db: db, prefix: pfx}, nil
		},
	})}, nil
}

// Store implements the [blob.StoreCloser] interface using PebbleDB.
type Store struct {
	*monitor.M[*pebble.DB, KV]
}

// Close implements part of the [blob.StoreCloser] interface.
func (s Store) Close(context.Context) (err error) {
	// Pebble has this silly behaviour where closing it a second time panics
	// instead of reporting an error. This is uncivilized, so handle the panic
	// and turn it into an error. If Pebble ever changes the error text this
	// will stop working, but at least it will report an error rather than
	// blowing up the whole program.
	defer func() {
		x := recover()
		if e, ok := x.(error); ok && e.Error() == "pebble: closed" {
			return // ok, fine, be that way
		} else if x != nil {
			err = fmt.Errorf("panic during close (recovered): %v", x)
		}
	}()
	return s.DB.Close()
}

// Options provides options for opening a Pebble database.
type Options struct{}

// KV implements the [blob.KV] interface using a Pebble database.
type KV struct {
	db     *pebble.DB
	prefix dbkey.Prefix
}

func (s KV) getRaw(key string) ([]byte, io.Closer, error) {
	val, c, err := s.db.Get([]byte(s.prefix.Add(key)))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil, blob.KeyNotFound(key)
	} else if err != nil {
		return nil, nil, err
	}
	return val, c, nil
}

// Get implements part of [blob.KV].
func (s KV) Get(_ context.Context, key string) (data []byte, err error) {
	val, c, err := s.getRaw(key)
	if err != nil {
		return nil, err
	}
	data = append([]byte{}, val...)
	c.Close() // required; invalidates val
	return data, nil
}

// Stat implements part of [blob.KV].
func (s KV) Stat(_ context.Context, keys ...string) (blob.StatMap, error) {
	out := make(blob.StatMap)
	for _, key := range keys {
		val, c, err := s.getRaw(key)
		if blob.IsKeyNotFound(err) {
			continue
		} else if err != nil {
			return nil, err
		}
		out[key] = blob.Stat{Size: int64(len(val))}
		c.Close()
	}
	return out, nil
}

// Put implements part of [blob.KV].
func (s KV) Put(_ context.Context, opts blob.PutOptions) error {
	key := []byte(s.prefix.Add(opts.Key))
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

// Delete implements part of [blob.KV].
func (s KV) Delete(ctx context.Context, key string) error {
	_, c, err := s.getRaw(key)
	if err != nil {
		return err
	}
	c.Close()
	return s.db.Delete([]byte(s.prefix.Add(key)), pebble.Sync)
}

// List implements part of [blob.KV].
func (s KV) List(ctx context.Context, start string, f func(string) error) error {
	bstart := []byte(s.prefix.Add(start))
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: bstart})
	if err != nil {
		return err
	}
	for it.First(); it.Valid(); it.Next() {
		if !bytes.HasPrefix(it.Key(), []byte(s.prefix)) {
			break
		}
		err := f(s.prefix.Remove(string(it.Key())))
		if errors.Is(err, blob.ErrStopListing) {
			break
		} else if err != nil {
			it.Close()
			return err
		}

		if err := ctx.Err(); err != nil {
			it.Close()
			return err
		}
	}
	return it.Close()
}

// Len implements part of [blob.KV].
func (s KV) Len(ctx context.Context) (int64, error) {
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte(s.prefix)})
	if err != nil {
		return 0, err
	}
	var count int64
	for it.First(); it.Valid(); it.Next() {
		if !bytes.HasPrefix(it.Key(), []byte(s.prefix)) {
			break
		}
		count++
	}
	return count, it.Close()
}
