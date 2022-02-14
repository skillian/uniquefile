package main

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	_ "github.com/alexbrainman/odbc"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/mattn/go-sqlite3"

	"github.com/skillian/argparse"
	"github.com/skillian/expr/errors"
	"github.com/skillian/expr/stream/sqlstream"
	"github.com/skillian/logging"
	"github.com/skillian/uniquefile"
	"github.com/skillian/uniquefile/sqlrepo"
)

const loggerNames = "expr/stream/sqlstream"

var (
	loggers, logger = func() ([]*logging.Logger, *logging.Logger) {
		lock := &sync.Mutex{}
		names := strings.Split(loggerNames, ",")
		loggers := make([]*logging.Logger, len(names))
		for i, name := range names {
			logger := logging.GetLogger(name)
			h := &logging.LockHandler{
				Locker:  lock,
				Handler: &logging.ConsoleHandler{},
			}
			h.SetFormatter(logging.GoFormatter{})
			h.SetLevel(logging.VerboseLevel)
			logger.AddHandler(h)
			logger.SetLevel(logging.VerboseLevel)
			loggers[i] = logger
		}
		return loggers, loggers[len(loggers)-1]
	}()
)

type Config struct {
	DB struct {
		DriverName     string `json:"driverName"`
		DataSourceName string `json:"dataSourceName"`
		Dialect        string `json:"dialect"`
	} `json:"db"`
}

func main() {
	me, err := user.Current()
	if err != nil {
		panic(errors.Errorf0From(
			err, "failed to determine current user",
		))
	}
	defaultWorkers := runtime.NumCPU() * 3 / 4
	if defaultWorkers == 0 {
		defaultWorkers = 1
	}
	parser := argparse.MustNewArgumentParser(
		argparse.Description(
			"identify unique files in a system",
		),
	)
	var uriStrings []string
	parser.MustAddArgument(
		argparse.MetaVar("URI"),
		argparse.ActionFunc(argparse.Append),
		argparse.Nargs(1),
		argparse.Help("one or more URIs to scan through"),
	).MustBind(&uriStrings)
	_ = parser.MustAddArgument(
		argparse.OptionStrings("--log-level"),
		argparse.MetaVar("LOG_LEVEL"),
		argparse.ActionFunc(argparse.Store),
		argparse.Default(logging.WarnLevel),
		argparse.Type(func(v string) (interface{}, error) {
			if lvl, ok := logging.ParseLevel(v); ok {
				for _, logger := range loggers {
					logger.SetLevel(lvl)
				}
				return lvl, nil
			}
			return nil, errors.Errorf1(
				"invalid logging level: %q", v,
			)
		}),
	)
	var logFileCloser func()
	_ = parser.MustAddArgument(
		argparse.OptionStrings("--log-file"),
		argparse.MetaVar("LOG_LEVEL"),
		argparse.ActionFunc(argparse.Store),
		argparse.Type(func(v string) (interface{}, error) {
			var f *os.File
			var err error
			if _, err2 := os.Stat(v); os.IsNotExist(err2) {
				f, err = os.Create(v)
			} else {
				f, err = os.OpenFile(v, os.O_RDWR|os.O_APPEND, 0640)
			}
			if err != nil {
				return nil, errors.Errorf1From(
					err, "failed to open logging file: %v",
					v,
				)
			}
			logFileCloser = func() {
				if err := f.Close(); err != nil {
					logger.LogErr(
						errors.Errorf1From(
							err, "failed to close log file: %v",
							v,
						),
					)
				}
			}
			h := logging.NewWriterHandler(f, &sync.Mutex{})
			h.SetFormatter(logging.DefaultFormatter{})
			// TODO: Set logger level?
			for _, logger := range loggers {
				logger.AddHandler(h)
			}
			return v, nil
		}),
	)
	var workers int
	parser.MustAddArgument(
		argparse.OptionStrings("-w", "--workers"),
		argparse.MetaVar("NUM_WORKERS"),
		argparse.ActionFunc(argparse.Store),
		argparse.Type(argparse.Int),
		argparse.Default(defaultWorkers),
		argparse.Help(
			"limit the number of workers (default: %d)",
			defaultWorkers,
		),
	).MustBind(&workers)
	var indicatorNames []string
	parser.MustAddArgument(
		argparse.OptionStrings("-i", "--indicator"),
		argparse.MetaVar("INDICATOR"),
		argparse.ActionFunc(argparse.Append),
		argparse.Nargs(1),
		argparse.Help(
			"indicators to use to scan files",
		),
	).MustBind(&indicatorNames)
	var createDB bool
	parser.MustAddArgument(
		argparse.OptionStrings("-I", "--initialize"),
		argparse.ActionFunc(argparse.StoreTrue),
		argparse.Help(
			"initialize the database",
		),
	).MustBind(&createDB)
	_ = parser.MustParseArgs()
	configFile := filepath.Join(me.HomeDir, ".config", "uniquefile.json")
	if logFileCloser != nil {
		defer logFileCloser()
	}
	if err := main2(
		configFile, uriStrings, workers,
		indicatorNames, createDB,
	); err != nil {
		panic(err)
	}
}

type scanner func(ctx context.Context, root uniquefile.URI, files chan indicationRequest)

var scanners = map[string]scanner{
	"file": scanLocalFiles,
}

func main2(
	configFile string, uriStrings []string, workers int,
	indicatorNames []string, createDB bool,
) error {
	type uriScanner struct {
		uri     uniquefile.URI
		scanner scanner
	}
	uris := make([]uriScanner, len(uriStrings))
	for i, uriStr := range uriStrings {
		if err := uris[i].uri.FromString(uriStr); err != nil {
			return errors.Errorf1From(
				err, "failed to parse %q as a URI",
				uriStr,
			)
		}
		var ok bool
		uris[i].scanner, ok = scanners[uris[i].uri.Scheme]
		if !ok {
			return errors.Errorf1(
				"URI scheme %q is not supported",
				uris[i].uri.Scheme,
			)
		}
	}
	indicators := make([]uniquefile.Indicator, len(indicatorNames))
	for i, indStr := range indicatorNames {
		var ok bool
		indicators[i], ok = uniquefile.ParseIndicator(indStr)
		if !ok {
			return errors.Errorf1(
				"no such indicator: %q", indStr,
			)
		}
	}
	var cfg Config
	{
		bs, err := ioutil.ReadFile(configFile)
		if err != nil {
			return errors.Errorf1From(
				err, "failed to read configuration file: %v",
				configFile,
			)
		}
		if err := json.Unmarshal(bs, &cfg); err != nil {
			return errors.Errorf1From(
				err, "failed to parse configuration file: %v",
				configFile,
			)
		}
	}
	di, err := sqlstream.ParseDialect(cfg.DB.Dialect)
	if err != nil {
		return errors.Errorf1From(
			err, "failed to parse %q as a SQL dialect",
			cfg.DB.Dialect,
		)
	}
	ctx := context.Background()
	r, err := sqlrepo.OpenRepo(
		ctx, cfg.DB.DriverName, cfg.DB.DataSourceName,
		sqlstream.WithDialect(di),
	)
	if createDB {
		logger.Verbose0("creating database schema...")
		if err := r.DB().CreateCollection(ctx, &sqlrepo.Resource{}); err != nil {
			return err
		}
		if err := r.DB().CreateCollection(ctx, &sqlrepo.Indication{}); err != nil {
			return err
		}
		logger.Verbose0("done creating database schema.")
	}
	if err != nil {
		return errors.Errorf0From(
			err, "failed to connect to database",
		)
	}
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()
	requests := make(chan indicationRequest, 1024)
	results := make(chan indictionResult, 1024)
	repoCh := make(chan struct{})
	logger.Verbose0("starting repository goroutine...")
	go func() {
		defer close(repoCh)
		defer logger.Verbose0("stopping repository goroutine...")
		for res := range results {
			logger.Verbose("got indication result: %#v", res)
			if res.err != nil {
				logger.Error2(
					"error while calculating "+
						"indication for %v: %v",
					res.uri, res.err,
				)
			} else {
				if err := r.SetIndications(ctx, res.uri, res.ind); err != nil {
					logger.LogErr(
						errors.Errorf2From(
							err, "failed to set %v's "+
								"indications to %v",
							res.uri, res.ind,
						),
					)
					cancel()
					return
				}
			}
			uniquefile.PutIndication(&res.ind)
		}
	}()
	var indicatorWg sync.WaitGroup
	indicatorWg.Add(workers)
	for i := 0; i < workers; i++ {
		logger.Verbose0("starting indicator goroutine...")
		go func() {
			defer indicatorWg.Done()
			defer logger.Verbose0("stopping indicator goroutine...")
			scanReadSeekClosers(ctx, indicators, requests, results)
		}()
	}
	var readerWg sync.WaitGroup
	for _, uri := range uris {
		readerWg.Add(1)
		logger.Verbose0("starting reader goroutine...")
		uri := uri
		go func() {
			defer readerWg.Done()
			defer logger.Verbose0("stopping reader goroutine...")
			uri.scanner(ctx, uri.uri, requests)
		}()
	}
	readerWg.Wait()
	logger.Verbose0("stopped reader goroutines.")
	close(requests)
	indicatorWg.Wait()
	logger.Verbose0("stopped indicator goroutines.")
	close(results)
	<-repoCh
	logger.Verbose0("stopped repository goroutine.")
	return nil
}

func scanLocalFiles(ctx context.Context, root uniquefile.URI, uris chan indicationRequest) {
	p := filePathOf(root)
	f, err := os.Open(p)
	if err != nil {
		logger.LogErr(errors.Errorf1From(
			err, "failed to open directory %v for reading",
			p,
		))
		return
	}
	defer func() {
		if err := f.Close(); err != nil {
			logger.LogErr(errors.Errorf1From(
				err, "error while closing directory %v",
				p,
			))
		}
	}()
	for {
		entries, err := f.Readdir(1024)
		if err != nil {
			if err == io.EOF {
				return
			}
			logger.LogErr(errors.Errorf1From(
				err, "failed to read next batch of entries from %v",
				p,
			))
			return
		}
		for _, entry := range entries {
			fullpath := filepath.Join(p, entry.Name())
			if entry.IsDir() {
				scanLocalFiles(ctx, uriOfFilePath(fullpath), uris)
			} else {
				uris <- indicationRequest{
					uri: uriOfFilePath(fullpath),
					rsc: func() (io.ReadSeekCloser, error) {
						f, err := os.Open(fullpath)
						if err != nil {
							return nil, err // nil io.ReadSeekCloser
						}
						return f, nil
					},
				}
			}
		}
	}
}

type indicationRequest struct {
	uri uniquefile.URI
	rsc func() (io.ReadSeekCloser, error)
}

type indictionResult struct {
	uri uniquefile.URI
	ind *uniquefile.Indication
	err error
}

func scanReadSeekClosers(
	ctx context.Context,
	indicators []uniquefile.Indicator,
	requests chan indicationRequest,
	results chan indictionResult,
) {
	logger.Verbose0("entered scanReadSeekClosers")
	defer logger.Verbose0("exited scanReadSeekClosers")
	for req := range requests {
		logger.Verbose1("got request: %#v", req)
		if err := ctx.Err(); err != nil {
			results <- indictionResult{
				uri: req.uri,
				err: err,
			}
			logger.Info("scanReadSeekClosers goroutine shutting down")
			return
		}
		ind, err := func() (ind *uniquefile.Indication, Err error) {
			ind = uniquefile.NewIndication()
			rsc, err := req.rsc()
			if err != nil {
				return nil, err
			}
			defer errors.Catch(&Err, rsc.Close)
			start, err := rsc.Seek(0, io.SeekCurrent)
			if err != nil {
				return nil, errors.Errorf1From(
					err, "failed to determine current "+
						"offset of %v",
					rsc,
				)
			}
			for _, ir := range indicators {
				if err := ir.Indicate(ctx, rsc, ind); err != nil {
					return nil, err
				}
				if _, err := rsc.Seek(start, io.SeekStart); err != nil {
					return nil, errors.Errorf1From(
						err, "failed to rewind %v for "+
							"next indication",
						req.uri,
					)
				}
			}
			return ind, nil
		}()
		res := indictionResult{
			uri: req.uri,
			ind: ind,
			err: err,
		}
		results <- res
	}
}

func filePathOf(u uniquefile.URI) string {
	if runtime.GOOS == "windows" {
		// turn it into a UNC path:
		sb := strings.Builder{}
		if u.Hostname != "" {
			sb.WriteString("\\\\")
			sb.WriteString(u.Hostname)
			sb.WriteByte('\\')
		}
		parts := strings.Split(u.Path, "/")[1:]
		// rewrite drive letter to administrative share:
		if u.Hostname != "" && len(parts) >= 2 && len(parts[0]) == 2 && parts[0][1] == ':' {
			parts[0] = parts[0][:1] + "$"
		}
		for i, part := range parts {
			if i > 0 {
				sb.WriteByte('\\')
			}
			sb.WriteString(part)
		}
		return sb.String()
	}
	return u.String()
}

func uriOfFilePath(fullPath string) (u uniquefile.URI) {
	u.Scheme = "file"
	if runtime.GOOS == "windows" {
		if strings.HasPrefix(fullPath, "\\\\") {
			n := strings.IndexByte(fullPath[len("\\\\"):], '\\') + len("\\\\")
			u.Hostname = fullPath[len("\\\\"):n]
			fullPath = fullPath[n:]
			if len(fullPath) >= 2 && fullPath[2] == '$' {
				// change admin share back to local:
				fullPath = fullPath[:2] + ":" + fullPath[3:]
			}
		}
		u.Path = strings.ReplaceAll(fullPath, "\\", "/")
		if !strings.HasPrefix(u.Path, "/") {
			u.Path = "/" + u.Path
		}
		return
	}
	if err := u.FromString(fullPath); err != nil {
		panic(err)
	}
	return
}
