package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httputil"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
)

var (
	// logger for verbose mode
	vlog *log.Logger
	// logger for errors
	elog *log.Logger
	// logger for debug mode
	dbglog *log.Logger
)

// TODO: Expires field is ignored, update it

var errUnchanged = errors.New("unchanged")

type decoder interface {
	suitable(mime string) bool
	validate(data []byte) error
}

type xmlDecoder struct{}

func (xmlDecoder) suitable(mime string) bool {
	return strings.Contains(mime, "text/xml")
}

func (xmlDecoder) validate(data []byte) error {
	var v interface{}
	if err := xml.Unmarshal(data, &v); err != nil {
		return fmt.Errorf("invalid XML: %v", err)
	}
	return nil
}

type jsonDecoder struct{}

func (jsonDecoder) suitable(mime string) bool {
	return strings.Contains(mime, "application/") && strings.Contains(mime, "json")
}

func (jsonDecoder) validate(data []byte) error {
	var v interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return fmt.Errorf("invalid JSON: %v", err)
	}
	return nil
}

// slice of all implemented decoders
var decoders = []decoder{
	&xmlDecoder{},
	&jsonDecoder{},
}

// detectDecoder returns a suitable decoder to handle data described by content-type ctype.
// detectDecoder returns nil if no suitable decoder is found.
func detectDecoder(ctype string) decoder {
	for _, dec := range decoders {
		if dec.suitable(ctype) {
			return dec
		}
	}
	return nil
}

// entry repesents a fetched source result and it's error if any processing on the entry failed.
type entry struct {
	url, etag string
	content   []byte
	headers   []byte
	date      time.Time
	expires   time.Time
	decoder   decoder
	err       error
}

func newEntry(url, etag, date, expires string, content []byte) (*entry, error) {
	e := &entry{
		url:     url,
		etag:    etag,
		content: content,
	}
	var err error
	n, err := strconv.ParseInt(date, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("date from database is not a UNIX timestamp: %v", err)
	}
	e.date = time.Unix(n, 0)
	n, err = strconv.ParseInt(date, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("expires from database is not a UNIX timestamp: %v", err)
	}
	e.expires = time.Unix(n, 0)
	return e, nil
}

// httpGet performs a GET request for entry e, optionally using Etag.
// It returns a new entry object on success or nil and an error.
// In case of unchanged data, errUnchanged is returned.
func (e *entry) httpGet(hc *http.Client) (*entry, error) {
	ne := &entry{} // resulting entry
	req, err := http.NewRequest("GET", e.url, nil)
	if err != nil {
		return nil, fmt.Errorf("cannot create HTTP request: %v", err)
	}
	if e.etag != "" {
		req.Header.Add("If-None-Match", e.etag)
	}
	req.Header.Set("User-Agent", "vrsk/0.1")
	resp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cannot GET from HTTP: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotModified {
			err = errUnchanged
		} else {
			err = fmt.Errorf("server returned status: %s", resp.Status)
		}
		io.Copy(ioutil.Discard, resp.Body)
		return nil, err
	}
	ne.content, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("cannot read from HTTP: %s", err)
	}
	ne.headers, err = httputil.DumpResponse(resp, false)
	if err != nil {
		return nil, fmt.Errorf("cannot dump HTTP response: %s", err)
	}
	ne.etag = strings.Trim(resp.Header.Get("Etag"), "\"")
	ne.decoder = detectDecoder(resp.Header.Get("Content-Type"))
	return ne, nil
}

// equal returns true when ne has the same content and content-type as e.
// Content-type that is not checked by a decoder is ignored in the comparison.
func (e *entry) equal(ne *entry) bool {
	if e.decoder != nil && e.decoder != ne.decoder {
		return false
	}
	return bytes.Equal(e.content, ne.content)
}

// valid returns an error if data is not valid for the decoder selected by the entry content-type.
// Typically, that means XML responses must be valid XML, JSON responses valid JSON, and so on.
// Data of a content-type that has no available decoder is considered valid.
func (e *entry) valid() error {
	// No suitable decoder means valid
	if e.decoder == nil {
		return nil
	}
	return e.decoder.validate(e.content)
}

// refresh fetches an entry's URL using hc as Client. Errors are saved inside the entry itself.
func (e *entry) refresh(hc *http.Client) {
	ne, err := e.httpGet(hc)
	if err != nil {
		e.err = err
		return
	}
	// For those who don't use etags, same content means unchanged
	if e.equal(ne) {
		e.err = errUnchanged
		return
	}
	if err := e.valid(); err != nil {
		e.err = fmt.Errorf("invalid element not refreshed: %v", err)
		return
	}
	lasted := e.expires.Sub(e.date)
	e.err = nil
	e.content = ne.content
	e.headers = ne.headers
	e.etag = ne.etag
	e.decoder = ne.decoder
	e.date = time.Now()
	e.expires = e.date
	if lasted >= 0 {
		e.expires = e.expires.Add(lasted)
	}
}

// dbconn represents the connection to one DB source, regardless of the underlying DB connection.
type dbconn struct {
	db              *sql.DB
	name            string
	readQuery       string
	updateQuery     string
	updateTimeQuery string
	afterQueries    []string
	entries         chan *entry
}

// creates a DB connection from configuration cf applying default configuration values.
func newDbconn(name string, cf *DBConf) (*dbconn, error) {
	var err error
	c := &dbconn{
		name:    name,
		entries: make(chan *entry), // can buffer this channel
	}
	c.db, err = sql.Open("mysql", cf.DSN)
	if err != nil {
		return nil, fmt.Errorf("cannot open SQL database: %v", err)
	}
	if cf.URL == "" {
		cf.URL = "url"
	}
	if cf.Etag == "" {
		cf.Etag = "etag"
	}
	if cf.Date == "" {
		cf.Date = "tstamp"
	}
	if cf.Content == "" {
		cf.Content = "content"
	}
	if cf.Headers == "" {
		cf.Headers = "headers"
	}
	if cf.Expires == "" {
		cf.Expires = "expires"
	}
	c.readQuery = fmt.Sprintf("SELECT %s,%s,%s,%s,%s FROM %s WHERE %s <= UNIX_TIMESTAMP(NOW())", cf.URL, cf.Etag, cf.Date, cf.Expires, cf.Content, cf.Table, cf.Expires)
	c.updateQuery = fmt.Sprintf("UPDATE %s SET %s=?, %s=?, %s=?, %s=?, %s=? WHERE %s=?", cf.Table, cf.Etag, cf.Date, cf.Expires, cf.Content, cf.Headers, cf.URL)
	c.updateTimeQuery = fmt.Sprintf("UPDATE %s SET %s=?, %s=? WHERE %s=?", cf.Table, cf.Date, cf.Expires, cf.URL)
	c.afterQueries = cf.After
	return c, nil
}

// query fetches all source rows from the database and returns them as entries.
func (c *dbconn) query() ([]*entry, error) {
	var entries []*entry
	rows, err := c.db.Query(c.readQuery)
	if err != nil {
		return nil, fmt.Errorf("cannot query HTTP URLs: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		// Scan date as string and convert later to have better error message
		var (
			url, etag     string
			date, expires string
			content       []byte
		)
		if err := rows.Scan(&url, &etag, &date, &expires, &content); err != nil {
			return nil, fmt.Errorf("cannot read URL row: %v", err)
		}
		e, err := newEntry(url, etag, date, expires, content)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// ping is a convenience wrapper to ping the database and keep the connection open.
func (c *dbconn) ping() error {
	return c.db.Ping()
}

// set persists a source entry to the database.
func (c *dbconn) set(e *entry) error {
	rows, err := c.db.Query(c.updateQuery, e.etag, fmt.Sprintf("%d", e.date.Unix()), fmt.Sprintf("%d", e.expires.Unix()), e.content, e.headers, e.url)
	if err != nil {
		return fmt.Errorf("cannot update '%s' in DB: %v", e.url, err)
	}
	rows.Close()
	return nil
}

// set persists a source entry to the database.
func (c *dbconn) setMeta(e *entry) error {
	rows, err := c.db.Query(c.updateTimeQuery, fmt.Sprintf("%d", e.date.Unix()), fmt.Sprintf("%d", e.expires.Unix()), e.url)
	if err != nil {
		return fmt.Errorf("cannot update metadata '%s' in DB: %v", e.url, err)
	}
	rows.Close()
	return nil
}

// after runs all the queries configured to run after successful source update.
func (c *dbconn) after() {
	for i := range c.afterQueries {
		rows, err := c.db.Query(c.afterQueries[i])
		if err != nil {
			elog.Printf("%s: after update query failed: %s: %v", c.name, c.afterQueries[i], err)
			continue
		}
		rows.Close()
	}
}

// finalize reads n entries from the entries channel and processes them, either saving or
// displaying errors or information. Only when something is saved, dbconn.after() is called.
func (c *dbconn) finalize(n int) {
	var updated bool
	for i := 0; i < n; i++ {
		e := <-c.entries
		if e.err != nil {
			if e.err != errUnchanged {
				elog.Printf("%s: failed refreshing URL %s: %v", c.name, e.url, e.err)
			} else {
				dbglog.Printf("%s: %s unchanged", c.name, e.url)
				if err := c.setMeta(e); err != nil {
					elog.Printf("%s: failed refreshing unchanged URL: %v", c.name, e.err)
				}
			}
			continue
		}
		if err := c.set(e); err != nil {
			elog.Printf("%s: update: %s: %v", c.name, e.url, err)
			continue
		}
		vlog.Printf("%s: updated %s", c.name, e.url)
		updated = true
	}
	// Only execute after() is something actually changed
	if updated {
		c.after()
	}
}

// update fetches all sources from the DB with fetcher and saves the results.
func (c *dbconn) update(fetcher *fetcher) {
	// Ignore errors when pinging, as it might hit a closed connection
	c.ping()
	ents, err := c.query()
	if err != nil {
		elog.Printf("%s: %v", c.name, err)
		return
	}
	go func() {
		for i := range ents {
			fetcher.fetch(ents[i], c.entries)
		}
	}()
	c.finalize(len(ents))
}

// run waits for a new tick to wake up and updates all sources from it's connection.
// run marks the received WaitGroup when done.
func (c *dbconn) run(ftc *fetcher, ticker <-chan *sync.WaitGroup) {
	for wg := range ticker {
		c.update(ftc)
		wg.Done()
	}
}

type duration time.Duration

func (d *duration) UnmarshalJSON(data []byte) error {
	s := strings.Trim(string(data), "\"")
	t, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = duration(t)
	return nil
}

type JsonConf struct {
	Sleep   duration
	Systems map[string]*DBConf
}

type DBConf struct {
	DSN     string
	Table   string
	URL     string
	Etag    string
	Date    string
	Expires string
	Headers string
	Content string
	After   []string
}

// runner represents the agent that wakes every tick and refreshes all connections using a fetcher.
type runner struct {
	conns   []*dbconn
	fetcher *fetcher
}

func newRunner(cf *JsonConf, fts *fetcher) (*runner, error) {
	var conns []*dbconn
	for name, conf := range cf.Systems {
		c, err := newDbconn(name, conf)
		if err != nil {
			return nil, fmt.Errorf("%s: cannot start database connection: %v", name, err)
		}
		conns = append(conns, c)
	}
	return &runner{conns: conns, fetcher: fts}, nil
}

func (r *runner) wake(c *dbconn, wg *sync.WaitGroup) {
	c.update(r.fetcher)
	wg.Done()
}

func (r *runner) run(c <-chan struct{}) {
	wg := &sync.WaitGroup{}
	for range c {
		wg.Add(len(r.conns))
		for i := range r.conns {
			go r.wake(r.conns[i], wg)
		}
		wg.Wait()
	}
}

type fetcher struct {
	fns    chan func(*http.Client)
	client *http.Client
}

func newFetcher(nworkers int, c *http.Client) *fetcher {
	f := &fetcher{
		fns:    make(chan func(*http.Client)),
		client: c,
	}
	for i := 0; i < nworkers; i++ {
		go f.run(f.fns)
	}
	return f
}

func (f *fetcher) run(fns <-chan func(*http.Client)) {
	for fn := range fns {
		fn(f.client)
	}
}

func (f *fetcher) fetch(e *entry, ready chan<- *entry) {
	f.fns <- func(hc *http.Client) {
		e.refresh(hc)
		ready <- e
	}
}

func makeHttpClient() *http.Client {
	hc := &http.Client{}
	return hc
}

func loadJsonConf(fname string) (*JsonConf, error) {
	r, err := os.Open(fname)
	if err != nil {
		return nil, fmt.Errorf("cannot open JSON configuration file: %v", err)
	}
	defer r.Close()
	cf := &JsonConf{}
	dec := json.NewDecoder(r)
	if err = dec.Decode(cf); err != nil {
		return nil, fmt.Errorf("cannot decode JSON configuration file: %v", err)
	}
	return cf, nil
}

func signaltrap() <-chan os.Signal {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP)
	return c
}

func main() {
	nFetchers := flag.Int("fetchers", 5, "Number of parallel HTTP downloaders")
	cfname := flag.String("conf", "", "JSON configuration file")
	verbose := flag.Bool("verbose", false, "Be verbose")
	debug := flag.Bool("debug", false, "Show debugging messages")
	flag.Parse()
	vlogOut := ioutil.Discard
	dbglogOut := ioutil.Discard
	if *verbose {
		vlogOut = os.Stdout
	}
	if *debug {
		vlogOut = os.Stdout
		dbglogOut = os.Stdout
	}
	log.SetOutput(dbglogOut)
	elog = log.New(os.Stderr, "ERROR - ", log.LstdFlags)
	vlog = log.New(vlogOut, "INFO - ", log.LstdFlags)
	dbglog = log.New(dbglogOut, "DEBUG - ", log.LstdFlags)
	if *cfname == "" {
		elog.Fatal("fatal: option -conf is mandatory")
	}
	cf, err := loadJsonConf(*cfname)
	if err != nil {
		elog.Fatalf("Unrecoverable error: cannot use JSON configuration file %s: %v", *cfname, err)
	}
	mysql.SetLogger(dbglog)
	run, err := newRunner(cf, newFetcher(*nFetchers, makeHttpClient()))
	if err != nil {
		elog.Fatalf("Unrecoverable error: %v", err)
	}
	ticks := make(chan struct{})
	go func() {
		sin := signaltrap()
		tin := time.Tick(time.Duration(cf.Sleep))
		ticks <- struct{}{}
		for {
			select {
			case <-sin:
				ticks <- struct{}{}
			case <-tin:
				ticks <- struct{}{}
			}
		}
	}()
	run.run(ticks)
}
