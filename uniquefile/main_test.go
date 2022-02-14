package main

import (
	"testing"

	"github.com/skillian/uniquefile"
)

type filePathOfTest struct {
	source string
	target string
}

var filePathOfTests = []filePathOfTest{
	{"file:///C:/Users/Sean/Downloads", `C:\Users\Sean\Downloads`},
	{
		"file://skillian-pc.paperless/C:/Users/Sean/Downloads",
		`\\skillian-pc.paperless\C$\Users\Sean\Downloads`,
	},
}

func TestFilePathOf(t *testing.T) {
	for _, tc := range filePathOfTests {
		t.Run(tc.source, func(t *testing.T) {
			u := uniquefile.URI{}
			if err := u.FromString(tc.source); err != nil {
				t.Fatal(err)
			}
			s := filePathOf(u)
			if s != tc.target {
				t.Fatalf(
					"expected does not match actual:\n\t%v\n\t%v",
					tc.target,
					s,
				)
			}
			u2 := uriOfFilePath(s)
			if u != u2 {
				t.Fatalf(
					"expected does not match actual:\n\t%#v\n\t%#v",
					u, u2,
				)
			}
		})
	}
}
