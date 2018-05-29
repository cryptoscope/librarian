package badger

import (
	"io/ioutil"
	"os"

	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"

	"cryptoscope.co/go/librarian"
	"cryptoscope.co/go/librarian/test"
)

func init() {
	type newIndexFunc func(name string, tipe interface{}) (Index, error)

	newSeqSetterIdx := func(name string, tipe interface{}) (Index, error) {
		dir, err := ioutil.TempDir("", "badger")
		if err != nil {
			return nil, errors.Wrap(err, "error creating tempdir")
		}

		defer os.RemoveAll(dir)

		opts := badger.DefaultOptions
		opts.Dir = dir
		opts.ValueDir = dir

		db, err := badger.Open(opts)
		if err != nil {
			return nil, errors.Wrap(err, "error opening database")
		}

		return NewIndex(db, tipe), nil
	}

	toSetterIdx := func(f newIndexFunc) test.NewSetterIndexFunc {
		return func(name string, tipe interface{}) (librarian.SetterIndex, error) {
			return f(name, tipe)
		}
	}

	toSeqSetterIdx := func(f newIndexFunc) test.NewSeqSetterIndexFunc {
		return func(name string, tipe interface{}) (librarian.SeqSetterIndex, error) {
			return f(name, tipe)
		}
	}

	toSourceSetterIdx := func(f newIndexFunc) test.NewSourceSetterIndexFunc {
		return func(name string, tipe interface{}) (librarian.SourceSetterIndex, error) {
			return f(name, tipe)
		}
	}

	test.RegisterSourceSetterIndex("badger", toSourceSetterIdx(newSeqSetterIdx))
	test.RegisterSeqSetterIndex("badger", toSeqSetterIdx(newSeqSetterIdx))
	test.RegisterSetterIndex("badger", toSetterIdx(newSeqSetterIdx))
}
