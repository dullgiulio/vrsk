package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strconv"
	//"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type entry struct {
	url, etag string
	content   []byte
	date      time.Time
	err       error
}

func newEntry(url, etag, date string) (*entry, error) {
	e := &entry{
		url:  url,
		etag: etag,
	}
	var err error
	n, err := strconv.ParseInt(date, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("date from database is not a UNIX timestamp: %v", err)
	}
	e.date = time.Unix(n, 0)
	return e, nil
}

type dbconn struct {
	db          *sql.DB
	readQuery   string
	updateQuery string
	afterQuery  string
}

func newDbconn(cf *dbconf) (*dbconn, error) {
	var err error
	c := &dbconn{}
	c.db, err = sql.Open("mysql", cf.dsn)
	if err != nil {
		return nil, fmt.Errorf("cannot open SQL database: %v", err)
	}
	c.readQuery = fmt.Sprintf("SELECT %s,%s,%s FROM %s", cf.urlf, cf.etagf, cf.datef, cf.table)
	c.updateQuery = fmt.Sprintf("UPDATE %s SET %s=?, %s=?, %s=? WHERE %s=?", cf.table, cf.etagf, cf.datef, cf.contentf, cf.urlf)
	c.afterQuery = cf.after
	return c, nil
}

func (c *dbconn) query() ([]*entry, error) {
	var entries []*entry
	rows, err := c.db.Query(c.readQuery)
	if err != nil {
		return nil, fmt.Errorf("cannot query HTTP URLs: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		// Scan date as string and convert later to have better error message
		var url, etag, date string
		if err := rows.Scan(&url, &etag, &date); err != nil {
			return nil, fmt.Errorf("cannot read URL row: %v", err)
		}
		e, err := newEntry(url, etag, date)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func (c *dbconn) set(e *entry) error {
	rows, err := c.db.Query(c.updateQuery, e.etag, fmt.Sprintf("%d", e.date.Unix()), e.content, e.url)
	if err != nil {
		return fmt.Errorf("cannot update '%s' in DB: %v", e.url, err)
	}
	rows.Close()
	return nil
}

func (c *dbconn) after() error {
	// TODO
	return nil
}

func (c *dbconn) finalize(n int, ready <-chan *entry, done chan<- struct{}) {
	for i := 0; i < n; i++ {
		e := <-ready
		// TODO: if e.err, print error and continue
		if e.err != nil {
			log.Printf("update: failed refreshing %s: %v", e.url, err)
			continue
		}
		if err := c.set(e); err != nil {
			log.Printf("update: %v", err)
		}
		fmt.Printf("DEBUG: SET %s\n", e.url)
	}
	if err := c.after(); err != nil {
		log.Printf("after update: %v", err)
	}
	done <- struct{}{}
}

// TODO: pass logger
func (c *dbconn) update(es []*entry, fetchers chan<- func(), done chan<- struct{}) {
	subdone := make(chan *entry) // can buffer len(es)
	go c.finalize(len(es), subdone, done)
	for i := range es {
		fetchers <- fetch(es[i], subdone)
	}
}

func fetch(e *entry, ready chan<- *entry) func() {
	return func() {
		fmt.Printf("DEBUG: FETCH %s\n", e.url)
		ready <- e
	}
}

func joinDbflags(vs dbflag, j string) string {
	strs := make([]string, len(vs))
	for _, s := range vs {
		strs = append(strs, s.String())
	}
	return strings.Join(strs, j)
}

type dbconf struct {
	dsn      string
	table    string
	urlf     string
	etagf    string
	datef    string
	contentf string
	after    string
}

func (c *dbconf) String() string {
	return fmt.Sprintf("%s/%s,%s,%s,%s,%s", c.dsn, c.table, c.urlf, c.etagf, c.datef, c.contentf)
}

type dbflag []dbconf

func (f *dbflag) String() string {
	return joinDbflags(*f, " ")
}

func (f *dbflag) Set(s string) error {
	var cf dbconf
	p := strings.LastIndexByte(s, '/')
	if p < 0 {
		return fmt.Errorf("'%s' must include '/database/table,fields...'", s)
	}
	cf.dsn = s[0:p]
	parts := strings.SplitN(s[p+1:], ",", 5)
	cf.table = parts[0]
	fls := []*string{&cf.urlf, &cf.etagf, &cf.datef, &cf.contentf}
	defaults := []string{"url", "etag", "tstamp", "body"}
	for i := 0; i < len(fls); i++ {
		*fls[i] = defaults[i]
	}
	for i := 0; i < len(fls); i++ {
		if len(parts) <= i+1 {
			break
		}
		*fls[i] = parts[i+1]
	}
	*f = append(*f, cf)
	return nil
}

func (f dbflag) connect() ([]*dbconn, error) {
	var conns []*dbconn
	for i := range f {
		c, err := newDbconn(&f[i])
		if err != nil {
			return nil, fmt.Errorf("cannot start database connection: %v", err)
		}
		conns = append(conns, c)
	}
	return conns, nil
}

func fetcher(fns <-chan func()) {
	for fn := range fns {
		fn()
	}
}

func main() {
	//wait := 5 * time.Second
	nFetchers := 5
	var dbflags dbflag
	flag.Var(&dbflags, "db", "Database connection string in format user:password@tcp(localhost:3306)/database/table[,url-field,etag-field,date-field,content-field]")
	flag.Parse()
	conns, err := dbflags.connect()
	if err != nil {
		log.Fatalf("Unrecoverable error: %v", err)
	}
	done := make(chan struct{})
	fetchers := make(chan func())
	for i := 0; i < nFetchers; i++ {
		go fetcher(fetchers)
	}
	//for {
	for i := range conns {
		ents, err := conns[i].query()
		if err != nil {
			log.Printf("Error: %v", err) // TODO: print DSN etc
			continue
		}
		go conns[i].update(ents, fetchers, done)
		<-done
	}
	//	time.Sleep(wait)
	//}
}
