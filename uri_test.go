package uniquefile_test

import (
	"testing"

	"github.com/skillian/uniquefile"
)

type uriTest struct {
	source string
	uri    uniquefile.URI
}

var uriTests = []uriTest{
	{"file:opaque.txt", uniquefile.URI{
		Scheme: "file",
		Path:   "opaque.txt",
	}},
	{"file://server/share/file.txt", uniquefile.URI{
		Scheme:   "file",
		Hostname: "server",
		Path:     "/share/file.txt",
	}},
}

func TestURI(t *testing.T) {
	for _, tc := range uriTests {
		t.Run(tc.source, func(t *testing.T) {
			u := &uniquefile.URI{}
			if err := u.FromString(tc.source); err != nil {
				t.Fatal(err)
			}
			if *u != tc.uri {
				t.Fatalf(
					"actual URI does not match expected:\n\t%v\n\t%v",
					*u, tc.uri,
				)
			}
			s := u.String()
			if s != tc.source {
				t.Fatalf(
					"re-stringed URI doesn't match source:\n\t%v\n\t%v",
					s, tc.source,
				)
			}
		})
	}
}
