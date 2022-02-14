package uniquefile

import (
	"net/url"
	"strings"
)

const (
	FileScheme = "file"
)

// URI defines a resource that can be queried to obtain its indicators.
type URI struct {
	Scheme   string
	Hostname string
	Path     string
	Query    string
}

func (u *URI) FromString(s string) error {
	ur, err := url.Parse(s)
	if err != nil {
		return err
	}
	u.FromURL(ur)
	return nil
}

func (u *URI) FromURL(ur *url.URL) {
	u.Scheme = ur.Scheme
	if u.Scheme == "" {
		u.Scheme = FileScheme
	}
	u.Hostname = ur.Host
	if ur.Opaque != "" {
		u.Path = ur.Opaque
	} else {
		u.Path = ur.EscapedPath()
	}
	u.Query = ur.Query().Encode()
}

func (u *URI) String() string {
	parts := [...]string{
		u.Scheme,
		":",
		"", // maybe two slashes
		u.Hostname,
		"", // maybe slash
		u.Path,
		"", // maybe ?
		u.Query,
	}
	if u.Query != "" && !strings.HasPrefix(u.Query, "?") {
		parts[len(parts)-2] = "?"
	}
	if u.Hostname != "" {
		parts[2] = "//"
		if u.Path != "" && !strings.HasPrefix(u.Path, "/") {
			parts[4] = "/"
		}
	}
	return strings.Join(parts[:], "")
}
