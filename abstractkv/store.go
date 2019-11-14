package abstractkv

import (
	"context"
	"io"

	"go.cryptoscope.co/luigi"

	"go.cryptoscope.co/librarian/internal/errors"
)

type Store interface {
	io.Closer

	Get(key []byte) (value []byte, err error)
	Set(key, value []byte) error
	Delete(key []byte) error

	// this is an update transaction
	NewTransaction() (Transaction, error)
}

type ROTransaction interface {
	Get(key []byte) (value []byte, err error)
	Iterate(opts ...IterOption) (luigi.Source, error)

	Discard() error
}

type IterOption func(interface{}) error

type Transaction interface {
	ROTransaction

	Set(key, value []byte) error
	Delete(key []byte) error

	Rollback() error
	Commit() error
}

type ViewableStore interface {
	Store

	// this is a read-only transaction, not supported by every implementation
	NewROTransaction() (ROTransaction, error)
}

func Update(store Store, f func(Transaction) error) error {
	txn, err := store.NewTransaction()
	if err != nil {
		return err
	}
	defer txn.Discard()

	err = f(txn)
	if err != nil {
		rbErr := txn.Rollback()
		if rbErr != nil {
			return rbErr
		}
		return err
	}

	return txn.Commit()
}

func View(store ViewableStore, f func(ROTransaction) error) error {
	txn, err := store.NewROTransaction()
	if err != nil {
		return err
	}

	err = f(txn)
	if err != nil {
		return err
	}

	return txn.Discard()
}

func List(ctx context.Context, store Store) ([][]byte, error) {
	var list [][]byte

	f := func(txn ROTransaction) error {
		src, err := txn.Iterate()
		if err != nil {
			return err
		}

		var (
			v   interface{}
			key []byte
			ok  bool
		)

		for {
			v, err = src.Next(ctx)
			if luigi.IsEOS(err) {
				break
			} else if err != nil {
				return err
			}

			key, ok = v.([]byte)
			if !ok {
				return errors.TypeError{Expected: key, Actual: v}
			}

			list = append(list, key)
		}

		return nil
	}

	var err error

	if vstore, ok := store.(ViewableStore); ok {
		err = View(vstore, f)
	} else {
		err = Update(store, func(txn Transaction) error { return f(txn) })
	}

	return list, err
}
