package badger // import "cryptoscope.co/go/librarian/badger"

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"

	"cryptoscope.co/go/luigi"
	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"

	"cryptoscope.co/go/librarian"
)

type query struct {
	iterOpts badger.IteratorOptions
	iter     *badger.Iterator
	db       *badger.DB
	txn      *badger.Txn

	tipe interface{}

	seekTo librarian.Addr
	prefix librarian.Addr

	init, done sync.Once
	end        bool
}

func (qry *query) SeekTo(addr librarian.Addr) error {
	qry.seekTo = addr
	return nil
}

func (qry *query) WithPrefix(prefix librarian.Addr) error {
	qry.prefix = prefix

	if qry.seekTo == "" {
		qry.seekTo = prefix
	}

	return nil
}

func (qry *query) Reverse(rev bool) error {
	qry.iterOpts.Reverse = rev
	return nil
}

func (qry *query) initialize() {
	qry.txn = qry.db.NewTransaction(false)
	qry.iter = qry.txn.NewIterator(qry.iterOpts)

	qry.iter.Rewind()
	qry.iter.Seek([]byte(qry.seekTo))
}

func (qry *query) finalize() {
	qry.txn.Discard()
	qry.end = true
}

func (qry *query) Next(ctx context.Context) (interface{}, error) {
	qry.init.Do(qry.initialize)

	if qry.end || !qry.iter.ValidForPrefix([]byte(qry.prefix)) {
		qry.done.Do(qry.finalize)
		return nil, luigi.EOS{}
	}

	t := reflect.TypeOf(qry.tipe)
	v := reflect.New(t).Interface()

	item := qry.iter.Item()

	data, err := item.Value()
	if err != nil {
		return nil, errors.Wrap(err, "error getting value")
	}

	if um, ok := v.(librarian.Unmarshaler); ok {
		if t.Kind() != reflect.Ptr {
			v = reflect.ValueOf(v).Elem().Interface()
		}

		err = um.Unmarshal(data)
		err = errors.Wrap(err, "error unmarshaling using custom marshaler")
	} else {
		err = json.Unmarshal(data, v)
		err = errors.Wrap(err, "error unmarshaling using json marshaler")

		if t.Kind() != reflect.Ptr {
			v = reflect.ValueOf(v).Elem().Interface()
		}
	}
	defer qry.iter.Next()

	return librarian.KVPair{Key: librarian.Addr(item.Key()), Value: v}, err
}
