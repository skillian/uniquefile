package uniquefile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"io/fs"
	"sync"

	"github.com/skillian/errors"
)

// Bytes is just a string but its separate type makes it clear that
// the data is unlikely to be UTF-8 text.
type Bytes string

// Indicator reads r and populates ind with one or more indications.
type Indicator interface {
	// Indicate reads the reader and appends into bs an indication
	// which can be used to identify the uniqueness of the data.
	Indicate(ctx context.Context, r io.Reader, ind *Indication) error
}

// IndicatorCmper can be implemented by Indicators to compare
// indications it produced.  What that means depends on the indicator.
// For example, the LengthIndicator's Cmp compares the lengths (longer
// compares greater than lower).
type IndicatorCmper interface {
	// Keys returns the keys that this IndicatorCmper can compare
	Keys() []Bytes

	// Cmp compares values from an indicator.
	Cmp(ctx context.Context, key, a, b []byte) (int, error)
}

var (
	// ErrCannotCmp is returned when an IndicatorCmper is asked
	// to compare values whose keys it doesn't recognize (e.g.
	// if you pass CRC32s to the LengthIndicator).
	ErrCannotCmp = errors.New("cannot compare values of these keys")

	byteOrder = binary.BigEndian

	indicationCache = sync.Pool{
		New: func() interface{} {
			return &Indication{make([]byte, 0, 256)}
		},
	}
)

// Indication is a sequence of key and value byte slices.
type Indication struct {
	buf []byte
}

func NewIndication() *Indication {
	return indicationCache.Get().(*Indication)
}

// PutIndication puts an Indication back into the cache so it can be
// reused.
func PutIndication(i **Indication) {
	indicationCache.Put(*i)
	*i = nil
}

// Bytes accesses the byte representation of the indication directly.
func (i *Indication) Bytes() []byte { return i.buf }

// Reader creates a reader over the Indication that parses its keys
// and values out.
func (i *Indication) Reader() (reader interface {
	Next() (key, value []byte, err error)
}) {
	return &indicationReader{ind: i}
}

// Reset the Indication so it can be re-written into.  Do not call this
// while you have a reader over the indication.
func (i *Indication) Reset() { i.buf = i.buf[:0] }

// Write writes a key and value into the indication.
func (i *Indication) Write(key, value []byte) {
	writeSlice := func(i *Indication, bs []byte) {
		var buf [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(buf[:], uint64(len(bs)))
		i.buf = append(i.buf, buf[:n]...)
		i.buf = append(i.buf, bs...)
	}
	writeSlice(i, key)
	writeSlice(i, value)
}

type indicationReader struct {
	ind *Indication
	idx int
}

var _ io.ByteReader = (*indicationReader)(nil)

func (r *indicationReader) ReadByte() (byte, error) {
	if r.idx >= len(r.ind.buf) {
		return 0, io.EOF
	}
	b := r.ind.buf[r.idx]
	r.idx++
	return b, nil
}

func (r *indicationReader) Next() (key, value []byte, err error) {
	if r.idx == len(r.ind.buf) {
		return nil, nil, io.EOF
	}
	readSlice := func(r *indicationReader) (bs []byte, err error) {
		length, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		bs = r.ind.buf[r.idx : r.idx+int(length)]
		r.idx += int(length)
		return
	}
	key, err = readSlice(r)
	if err != nil {
		return
	}
	value, err = readSlice(r)
	return
}

type Indicators struct {
	reqs []chan *indicatorReq
	irs  []Indicator
}

type indicatorReq struct {
	ctx context.Context
	r   io.Reader
	ind *Indication
	err error
	wg  *sync.WaitGroup
}

var _ Indicator = (*Indicators)(nil)

func NewIndicators(irs ...Indicator) Indicator {
	idrs := &Indicators{
		reqs: make([]chan *indicatorReq, len(irs)),
		irs:  irs,
	}
	for i := range irs {
		idrs.reqs[i] = make(chan *indicatorReq)
		go func(i int) {
			for req := range idrs.reqs[i] {
				req.err = idrs.irs[i].Indicate(req.ctx, req.r, req.ind)
				if i > 0 {
					if err := req.r.(*io.PipeReader).Close(); err != nil {
						req.err = errors.CreateError(err, nil, req.err, 0)
					}
				}
				req.wg.Done()
			}
		}(i)
	}
	return idrs
}

func (irs *Indicators) Close() error {
	for i, ch := range irs.reqs {
		close(ch)
		irs.reqs[i] = nil
		irs.irs[i] = nil
	}
	irs.reqs = nil
	irs.irs = nil
	return nil
}

func (irs *Indicators) Indicate(ctx context.Context, r io.Reader, ind *Indication) error {
	var wg0, wg1 sync.WaitGroup
	wg0.Add(1)
	wg1.Add(len(irs.irs) - 1)
	reqs := make([]*indicatorReq, len(irs.irs))
	ws := make([]io.Writer, len(irs.irs)-1)
	for i := range irs.reqs {
		reqs[i] = &indicatorReq{
			ctx: ctx,
			ind: NewIndication(),
			err: nil,
		}
		// reqs[0] gets the tee'd reader
		if i > 0 {
			reqs[i].wg = &wg1
			reqs[i].r, ws[i-1] = io.Pipe()
		} else {
			reqs[i].wg = &wg0
		}
	}
	reqs[0].r = io.TeeReader(r, io.MultiWriter(ws...))
	for i, ch := range irs.reqs {
		ch <- reqs[i]
	}
	wg0.Wait()
	for _, w := range ws {
		w.(*io.PipeWriter).Close()
	}
	wg1.Wait()
	var errs error
	visited := map[Bytes]struct{}{}
	for _, req := range reqs {
		if req.err != nil {
			errs = errors.CreateError(req.err, nil, errs, 0)
			continue
		}
		r := req.ind.Reader()
		for {
			key, value, err := r.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				errs = errors.CreateError(err, nil, errs, 0)
			}
			bk := Bytes(key)
			if _, ok := visited[bk]; ok {
				continue
			}
			visited[bk] = struct{}{}
			ind.Write(key, value)
		}
	}
	for _, req := range reqs {
		PutIndication(&req.ind)
	}
	return errs
}

// CRC32Indicator computes the CRC32 of its data
var CRC32Indicator Indicator = hashAndLengthIndicator{
	hasher: func() hash.Hash { return crc32.NewIEEE() },
	key:    "crc32",
}

// SHA256Indicator computes the SHA-256 of its data
var SHA256Indicator Indicator = hashAndLengthIndicator{
	hasher: sha256.New,
	key:    "sha256",
}

type hashAndLengthIndicator struct {
	hasher func() hash.Hash
	key    string
}

func (ir hashAndLengthIndicator) Indicate(ctx context.Context, r io.Reader, ind *Indication) error {
	h := ir.hasher()
	length, err := copyContext(ctx, h, r, nil)
	if err != nil {
		return err
	}
	var buf [32]byte
	byteOrder.PutUint64(buf[:], uint64(length))
	ind.Write([]byte(lengthIndicatorKey), buf[:8])
	ind.Write([]byte(ir.key), h.Sum(buf[:0]))
	return nil
}

// LengthIndicator determines the length of the Reader in bytes.
// The resulting length is written out in big endian ("network") byte
// order.
var LengthIndicator interface {
	Indicator
	IndicatorCmper
} = lengthIndicator{}

const lengthIndicatorKey = "length"

var lengthIndicatorKeys = []Bytes{Bytes(lengthIndicatorKey)}

type lengthIndicator struct{}

func (lengthIndicator) Keys() []Bytes { return lengthIndicatorKeys }

func (lengthIndicator) Cmp(ctx context.Context, key, a, b []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if !bytes.Equal([]byte(lengthIndicatorKey), key) {
		return 0, ErrCannotCmp
	}
	ai, bi := byteOrder.Uint64(a), byteOrder.Uint64(b)
	switch {
	case ai == bi:
		return 0, nil
	case ai > bi:
		return 1, nil
	}
	return -1, nil
}

func (lengthIndicator) Indicate(ctx context.Context, r io.Reader, ind *Indication) error {
	var length int64
	switch r := r.(type) {
	case interface{ Len() int }:
		length = int64(r.Len())
	case interface{ Len() int64 }:
		length = r.Len()
	case interface{ Size() int64 }:
		length = r.Size()
	case interface{ Stat() (fs.FileInfo, error) }:
		st, err := r.Stat()
		if err != nil {
			return err
		}
		length = st.Size()
	case io.Seeker:
		// seek to the end to get the length and then seek
		// back to where we started.
		offset, err := r.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		length, err = r.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		_, err = r.Seek(offset, io.SeekStart)
		if err != nil {
			return err
		}
		length -= offset // we might be starting in the middle
	default:
		// if all else fails, just read the whole thing to
		// get the length.
		length2, err := copyContext(ctx, io.Discard, r, nil)
		if err != nil {
			return err
		}
		length = length2
	}
	var buf [8]byte
	byteOrder.PutUint64(buf[:], uint64(length))
	ind.Write([]byte(lengthIndicatorKey), buf[:])
	return nil
}

// copyContext is like io.Copy (or io.CopyBuffer if a buffer is
// provided) but if the context is cancelled when r.Read or w.Write is
// called, it will return.
func copyContext(ctx context.Context, w io.Writer, r io.Reader, buf []byte) (n int64, err error) {
	w = writerContext{ctx, w}
	r = readerContext{ctx, r}
	if buf == nil {
		return io.Copy(w, r)
	}
	return io.CopyBuffer(w, r, buf)
}

type readerContext struct {
	context.Context
	io.Reader
}

func (r readerContext) Read(p []byte) (int, error) {
	if err := r.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}

type writerContext struct {
	context.Context
	io.Writer
}

func (w writerContext) Write(p []byte) (int, error) {
	if err := w.Err(); err != nil {
		return 0, err
	}
	return w.Writer.Write(p)
}
