package uniquefile_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/skillian/uniquefile"
)

type indicationTest struct {
	name   string
	writes []indicationTestKvp
	reads  []indicationTestKvp
}

type indicationTestKvp struct {
	key   string
	value string
}

var indicationTests = []indicationTest{
	{
		name: "helloWorld",
		writes: []indicationTestKvp{
			{"hello", "world"},
		},
		reads: []indicationTestKvp{
			{"hello", "world"},
		},
	},
	{
		name: "helloWorldHello2World2",
		writes: []indicationTestKvp{
			{"hello", "world"},
			{"Hello2", "World2"},
		},
		reads: []indicationTestKvp{
			{"hello", "world"},
			{"Hello2", "World2"},
		},
	},
}

func TestIndication(t *testing.T) {
	ind := &uniquefile.Indication{}
	for _, tc := range indicationTests {
		ind.Reset()
		t.Run(tc.name, func(t *testing.T) {
			for _, wr := range tc.writes {
				ind.Write([]byte(wr.key), []byte(wr.value))
			}
			r := ind.Reader()
			for _, rd := range tc.reads {
				key, value, err := r.Next()
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(key, []byte(rd.key)) {
					t.Fatalf(
						"key does not match:\n\t%v\n\t%v",
						key, rd.key,
					)
				}
				if !bytes.Equal(value, []byte(rd.value)) {
					t.Fatalf(
						"value does not match:\n\t%v\n\t%v",
						value, rd.value,
					)
				}
			}
			k, v, err := r.Next()
			if err != io.EOF {
				t.Fatalf(
					"expected EOF, but got key: %v, value: %v",
					k, v,
				)
			}
		})
	}
}
