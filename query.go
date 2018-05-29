package librarian // import "cryptoscope.co/go/librarian"

import (
	"cryptoscope.co/go/luigi"
)

type KVPair struct {
	Key   Addr
	Value interface{}
}

type SourceIndex interface {
	Index

	Query(...QuerySpec) (luigi.Source, error)
}

type SourceSetterIndex interface {
	SetterIndex

	Query(...QuerySpec) (luigi.Source, error)
}

type Query interface {
	SeekTo(Addr) error
	Reverse(bool) error
	WithPrefix(Addr) error
}

type QuerySpec func(Query) error

func SeekTo(addr Addr) QuerySpec {
	return func(qry Query) error {
		return qry.SeekTo(addr)
	}
}

func Reverse(rev bool) QuerySpec {
	return func(qry Query) error {
		return qry.Reverse(rev)
	}
}

func WithPrefix(prefix Addr) QuerySpec {
	return func(qry Query) error {
		return qry.WithPrefix(prefix)
	}
}
