package uniquefile_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"github.com/skillian/uniquefile"
)

type indicatorTest struct {
	name   string
	ir     uniquefile.Indicator
	source string
	expect uniquefile.Indication
}

var (
	byteOrder = binary.BigEndian

	indicatorTests = []indicatorTest{
		func() (t indicatorTest) {
			var buf [8]byte
			t.name = "helloWorldLen"
			t.ir = uniquefile.LengthIndicator
			t.source = "hello, world!"
			byteOrder.PutUint64(buf[:], uint64(len(t.source)))
			t.expect.Write([]byte("length"), buf[:])
			return
		}(),
		func() (t indicatorTest) {
			var buf [8]byte
			t.name = "helloWorldCRC32"
			t.ir = uniquefile.CRC32Indicator
			t.source = "hello, world!"
			byteOrder.PutUint64(buf[:], uint64(len(t.source)))
			t.expect.Write([]byte("length"), buf[:])
			t.expect.Write([]byte("crc32"), []byte{88, 152, 141, 19})
			return
		}(),
		func() (t indicatorTest) {
			var buf [8]byte
			t.name = "helloWorldCRC32SHA256"
			t.ir = uniquefile.NewIndicators(
				uniquefile.CRC32Indicator,
				uniquefile.SHA256Indicator,
			)
			t.source = "hello, world!"
			byteOrder.PutUint64(buf[:], uint64(len(t.source)))
			t.expect.Write([]byte("length"), buf[:])
			t.expect.Write([]byte("crc32"), []byte{88, 152, 141, 19})
			t.expect.Write([]byte("sha256"), []byte{
				104, 230, 86, 178, 81, 230, 126, 131,
				88, 190, 248, 72, 58, 176, 213, 28, 102,
				25, 243, 231, 161, 169, 240, 231, 88,
				56, 212, 31, 243, 104, 247, 40,
			})
			return
		}(),
	}
)

func TestIndicator(t *testing.T) {
	for _, tc := range indicatorTests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			r := strings.NewReader(tc.source)
			res := uniquefile.NewIndication()
			if err := tc.ir.Indicate(ctx, r, res); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(res.Bytes(), tc.expect.Bytes()) {
				t.Fatalf(
					"result does not match expected:\n\t%v\n\t%v",
					res.Bytes(), tc.expect.Bytes(),
				)
			}
			if cr, ok := tc.ir.(io.Closer); ok {
				if err := cr.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
