package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"cryptoscope.co/go/librarian"
	"cryptoscope.co/go/luigi"
)

type NewSourceSetterIndexFunc func(name string, tipe interface{}) (librarian.SourceSetterIndex, error)

func TestSourceSetterIndex(newIdx NewSourceSetterIndexFunc) func(*testing.T) {
	return func(t *testing.T) {
		t.Run("Direct", TestSourceSetterIndexWithBreak(newIdx))
	}
}

func TestSourceSetterIndexWithBreak(newIdx NewSourceSetterIndexFunc) func(*testing.T) {
	return func(t *testing.T) {
		a := assert.New(t)
		r := require.New(t)
		ctx := context.Background()

		idx, err := newIdx(t.Name(), "str")
		r.NoError(err, "error returned by newIdx is not nil")
		r.NotNil(idx, "index returned by newIdx is nil")

		a.NoError(idx.Set(ctx, "stream0000", "foobar1"), "error returned by idx.Set is not nil")
		a.NoError(idx.Set(ctx, "stream0001", "fooomg2"), "error returned by idx.Set is not nil")
		a.NoError(idx.Set(ctx, "stream0003", " end  4"), "error returned by idx.Set is not nil")
		a.NoError(idx.Set(ctx, "stream0002", "onemor3"), "error returned by idx.Set is not nil")

		src, err := idx.Query(librarian.WithPrefix("stream"))
		r.NoError(err, "query returned an error")

		v, err := src.Next(ctx)
		t.Log("received", v)
		a.NoError(err, "next returned an error")
		a.Equal(librarian.KVPair{Key: "stream0000", Value: "foobar1"}, v, "unexpected value in stream")

		v, err = src.Next(ctx)
		t.Log("received", v)
		a.NoError(err, "next returned an error")
		a.Equal(librarian.KVPair{Key: "stream0001", Value: "fooomg2"}, v, "unexpected value in stream")

		v, err = src.Next(ctx)
		t.Log("received", v)
		a.NoError(err, "next returned an error")
		a.Equal(librarian.KVPair{Key: "stream0002", Value: "onemor3"}, v, "unexpected value in stream")

		v, err = src.Next(ctx)
		t.Log("received", v)
		a.NoError(err, "next returned an error")
		a.Equal(librarian.KVPair{Key: "stream0003", Value: " end  4"}, v, "unexpected value in stream")

		v, err = src.Next(ctx)
		t.Log("received", v)
		a.Equal(luigi.EOS{}, err, "expected end of stream")
		a.Nil(v, "expected nil but got", v)
	}
}

//func TestSourceIndexWithBreak(newLog mtest.NewLogFunc, newIdx NewSeqSetterIndexFunc) func(*testing.T) {
