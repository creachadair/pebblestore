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

package pebblestore_test

import (
	"flag"
	"io/ioutil"
	"os"
	"testing"

	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/pebblestore"
)

var keepOutput = flag.Bool("keep", false, "Keep test output after running")

func TestStore(t *testing.T) {
	dir, err := ioutil.TempDir("", "pebbletest")
	if err != nil {
		t.Fatalf("Creating temp directory: %v", err)
	}
	t.Logf("Test store: %s", dir)
	if !*keepOutput {
		defer os.RemoveAll(dir) // best effort cleanup
	}

	s, err := pebblestore.Open(dir, nil)
	if err != nil {
		t.Fatalf("Creating store at %q: %v", dir, err)
	}
	storetest.Run(t, s)
	if err := s.Close(); err != nil {
		t.Errorf("Closing store: %v", err)
	}
}
