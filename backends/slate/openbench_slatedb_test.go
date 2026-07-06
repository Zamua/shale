//go:build slatedb && integration

// openbench_slatedb_test.go: a MEASUREMENT harness (not a correctness
// test) for the fencing unit-open window: the OpenReplicaUnit duration a
// new owner pays before it can serve a handed-off replica position.
// During that window the displaced owner is fenced and the newcomer is
// not yet mounted, so at R=2 the window IS the write-unavailability a
// handoff causes. The harness produces the numbers a design decision
// about shrinking that window needs; it changes no production behavior.
//
// It measures the REAL production path (Handle.OpenReplicaUnit ->
// fenceEpochReplica -> DbBuilder.Build) against a REAL MinIO, and
// decomposes where the time goes two ways:
//
//  1. by shaping the unit's prior state per scenario (empty; flushed;
//     dirty WAL tail after an abrupt owner abandon; fragmented into many
//     small objects), and
//  2. by routing all slatedb object-store traffic through an in-process
//     counting reverse proxy that records every request's method, key
//     class (manifest / wal / compacted-SST / list), latency, and bytes
//     during the measured open.
//
// Candidate levers measured:
//
//   - L1 "shadow mount": a non-fencing DbReader open + reads immediately
//     before the fencing open (does reader pre-warm shrink the writer's
//     Build?);
//   - L2 "checkpoint before handoff": the displaced owner flushes its
//     memtable right before the newcomer opens (does a minimal WAL tail
//     shrink the Build?).
//
// Run (needs a running MinIO; several minutes at default sizes):
//
//	CGO_ENABLED=1 CGO_LDFLAGS="-L$HOME/.local/lib -lslatedb_uniffi" \
//	DYLD_LIBRARY_PATH="$HOME/.local/lib" \
//	go test -tags "slatedb integration" -run TestOpenBench -v -count=1 \
//	  -timeout 60m .
//
// Fixture env (same contract as the other MinIO integration tests):
// SLATE_MINIO_ENDPOINT (default http://localhost:9000),
// SLATE_MINIO_ACCESS / SLATE_MINIO_SECRET (default admin / supersecret).
//
// Harness knobs (env, all optional):
//
//	SLATE_OPENBENCH_ITERS    iterations per scenario (default 5)
//	SLATE_OPENBENCH_BASE     flushed base keys per unit (default 10000)
//	SLATE_OPENBENCH_TAIL     dirty unflushed tail keys (default 5000)
//	SLATE_OPENBENCH_SEQTAIL  sequential tail puts for the WAL-fragmented
//	                         scenario; each is its own awaited WAL write
//	                         (default 200)
//	SLATE_OPENBENCH_KEEP     "1" keeps the bench bucket for inspection
//
// NOTE this file lives in package slate (internal) deliberately: the
// harness needs the production Backing/Handle open path plus internal
// surfaces a benchmark must reach (resolveStore for the DbReader,
// (*Slate).db for the explicit memtable flush that models the L2 lever,
// dbNameReplica for the object census). It adds no exported API.

package slate

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	slatedb "slatedb.io/slatedb-go/uniffi"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

// ---------------------------------------------------------------------------
// knobs
// ---------------------------------------------------------------------------

func obEnvStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func obEnvInt(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func obIters() int   { return obEnvInt("SLATE_OPENBENCH_ITERS", 5) }
func obBase() int    { return obEnvInt("SLATE_OPENBENCH_BASE", 10000) }
func obTail() int    { return obEnvInt("SLATE_OPENBENCH_TAIL", 5000) }
func obSeqTail() int { return obEnvInt("SLATE_OPENBENCH_SEQTAIL", 200) }

const obValSize = 256
const obSeedConc = 64

// ---------------------------------------------------------------------------
// counting reverse proxy: every slatedb object-store request during the
// measured open is recorded with its key class, latency, and bytes.
// ---------------------------------------------------------------------------

type obReqStat struct {
	start     time.Time
	method    string
	path      string
	class     string // manifest | wal | sst | list | other
	status    int
	dur       time.Duration
	reqBytes  int64
	respBytes int64
}

type obProxy struct {
	url string

	mu    sync.Mutex
	stats []obReqStat
}

// take returns the recorded stats and clears the buffer.
func (p *obProxy) take() []obReqStat {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.stats
	p.stats = nil
	return out
}

func (p *obProxy) record(s obReqStat) {
	p.mu.Lock()
	p.stats = append(p.stats, s)
	p.mu.Unlock()
}

// obClassify buckets an S3 request (or a bare object key for the census)
// into the slatedb object classes that matter for the decomposition.
func obClassify(path string, query url.Values) string {
	if query != nil && query.Has("list-type") {
		return "list"
	}
	switch {
	case strings.Contains(path, "manifest/"):
		return "manifest"
	case strings.Contains(path, "wal/"):
		return "wal"
	case strings.Contains(path, "compacted/"):
		return "sst"
	default:
		return "other"
	}
}

type obRespRecorder struct {
	http.ResponseWriter
	status int
	n      int64
}

func (r *obRespRecorder) WriteHeader(c int) {
	r.status = c
	r.ResponseWriter.WriteHeader(c)
}

func (r *obRespRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.n += int64(n)
	return n, err
}

func (r *obRespRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// startObProxy starts a loopback reverse proxy in front of the MinIO
// endpoint. The Director rewrites only the URL, NOT req.Host, so the
// SigV4 signature the slatedb object_store client computed (over the
// proxy's host) still validates at MinIO (which checks the Host header
// the request carries, not its own address).
func startObProxy(t *testing.T, target string) *obProxy {
	t.Helper()
	tu, err := url.Parse(target)
	if err != nil {
		t.Fatalf("openbench: parse MinIO endpoint %q: %v", target, err)
	}
	p := &obProxy{}
	rp := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = tu.Scheme
			req.URL.Host = tu.Host
		},
		// A reader/writer shutdown cancels its in-flight requests;
		// that is expected, keep the test output clean.
		ErrorLog: log.New(io.Discard, "", 0),
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &obRespRecorder{ResponseWriter: w}
		cls := obClassify(r.URL.Path, r.URL.Query())
		reqBytes := r.ContentLength
		if reqBytes < 0 {
			reqBytes = 0
		}
		rp.ServeHTTP(rec, r)
		p.record(obReqStat{
			start:     start,
			method:    r.Method,
			path:      r.URL.Path,
			class:     cls,
			status:    rec.status,
			dur:       time.Since(start),
			reqBytes:  reqBytes,
			respBytes: rec.n,
		})
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("openbench: proxy listen: %v", err)
	}
	srv := &http.Server{Handler: handler}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	p.url = "http://" + ln.Addr().String()
	return p
}

// ---------------------------------------------------------------------------
// fixture
// ---------------------------------------------------------------------------

type obFixture struct {
	t       *testing.T
	mc      *minio.Client // direct to MinIO (bucket admin + census; bypasses the proxy)
	bucket  string
	backing *Backing
	proxy   *obProxy

	unitSeq atomic.Int64
}

func newObFixture(t *testing.T) *obFixture {
	t.Helper()

	endpoint := obEnvStr("SLATE_MINIO_ENDPOINT", "http://localhost:9000")
	access := obEnvStr("SLATE_MINIO_ACCESS", "admin")
	secret := obEnvStr("SLATE_MINIO_SECRET", "supersecret")

	host := strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	mc, err := minio.New(host, &minio.Options{
		Creds:  credentials.NewStaticV4(access, secret, ""),
		Secure: false,
	})
	if err != nil {
		t.Fatalf("openbench: minio client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bucket := fmt.Sprintf("shale-openbench-%d", time.Now().UnixNano())
	if err := mc.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		t.Fatalf("openbench: create bucket %q (is MinIO running at %s?): %v", bucket, endpoint, err)
	}
	t.Cleanup(func() {
		if os.Getenv("SLATE_OPENBENCH_KEEP") == "1" {
			t.Logf("openbench: keeping bucket %s (SLATE_OPENBENCH_KEEP=1)", bucket)
			return
		}
		cctx, ccancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer ccancel()
		objCh := mc.ListObjects(cctx, bucket, minio.ListObjectsOptions{Recursive: true})
		rmCh := make(chan minio.ObjectInfo)
		go func() {
			defer close(rmCh)
			for o := range objCh {
				rmCh <- o
			}
		}()
		for rErr := range mc.RemoveObjects(cctx, bucket, rmCh, minio.RemoveObjectsOptions{}) {
			if rErr.Err != nil {
				t.Logf("openbench: remove object %s: %v", rErr.ObjectName, rErr.Err)
			}
		}
		if err := mc.RemoveBucket(cctx, bucket); err != nil {
			t.Logf("openbench: remove bucket %q: %v", bucket, err)
		}
	})

	// All slatedb traffic goes through the counting proxy. NB NewBacking
	// writes the AWS_* env vars process-wide, so from here on every
	// slatedb open in this process points at the proxy (single Backing
	// per process, same constraint as production).
	proxy := startObProxy(t, endpoint)
	b, err := NewBacking(BackingConfig{
		Bucket:    bucket,
		Endpoint:  proxy.url,
		Region:    "us-east-1",
		AccessKey: access,
		SecretKey: secret,
		UseSSL:    false,
	})
	if err != nil {
		t.Fatalf("openbench: new backing: %v", err)
	}

	return &obFixture{t: t, mc: mc, bucket: bucket, backing: b, proxy: proxy}
}

// nextUnit returns a fresh, never-used replica position. Every iteration
// gets its own so no in-process slatedb epoch state carries across
// iterations (re-opening a previously fenced prefix in-process is not
// supported by slatedb).
func (fx *obFixture) nextUnit() storageunit.ReplicaUnit {
	id := fx.unitSeq.Add(1)
	return storageunit.NewReplicaUnit(storageunit.NewGenUnit(0, storageunit.UnitID(id)), 0)
}

// ---------------------------------------------------------------------------
// seeding + shaping helpers
// ---------------------------------------------------------------------------

func obKey(i int) []byte { return []byte(fmt.Sprintf("bench:%08d", i)) }

// obVal is a deterministic, mildly incompressible value of obValSize bytes.
func obVal(i int) []byte {
	v := make([]byte, obValSize)
	x := uint32(i)*2654435761 + 12345
	for j := range v {
		x = x*1664525 + 1013904223
		v[j] = byte(x >> 24)
	}
	return v
}

// seedBatched writes [start, start+n) through awaited WriteBatches of 500.
// Fast base seeding: the end state (bytes in WAL, then flushed to L0 by
// the caller's explicit memtable flush) is what the scenarios shape.
func seedBatched(t *testing.T, be backend.Backend, start, n int) {
	t.Helper()
	s := be.(*Slate)
	const batch = 500
	for off := 0; off < n; off += batch {
		end := off + batch
		if end > n {
			end = n
		}
		wb := slatedb.NewWriteBatch()
		for i := start + off; i < start+end; i++ {
			if err := wb.Put(obKey(i), obVal(i)); err != nil {
				t.Fatalf("openbench: batch put: %v", err)
			}
		}
		if _, err := s.db.WriteWithOptions(wb, slatedb.WriteOptions{AwaitDurable: true}); err != nil {
			t.Fatalf("openbench: batch write: %v", err)
		}
		wb.Destroy()
	}
}

// seedConcurrent writes [start, start+n) through the PRODUCTION per-write
// path (backend.Put, awaited) with conc workers, so awaited writes group
// on the WAL flush tick the way concurrent production traffic does.
func seedConcurrent(t *testing.T, be backend.Backend, start, n, conc int) {
	t.Helper()
	var next atomic.Int64
	next.Store(int64(start))
	limit := int64(start + n)
	var wg sync.WaitGroup
	errCh := make(chan error, conc)
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := next.Add(1) - 1
				if i >= limit {
					return
				}
				if err := be.Put(obKey(int(i)), obVal(int(i))); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatalf("openbench: concurrent seed: %v", err)
	default:
	}
}

// seedSequential writes [start, start+n) one awaited put at a time; each
// put lands (roughly) in its own WAL flush, fragmenting the tail into
// many small WAL objects.
func seedSequential(t *testing.T, be backend.Backend, start, n int) {
	t.Helper()
	for i := start; i < start+n; i++ {
		if err := be.Put(obKey(i), obVal(i)); err != nil {
			t.Fatalf("openbench: sequential seed %d: %v", i, err)
		}
	}
}

// flushMemtable is the L2 lever primitive: force the owner's memtable
// (and immutable memtables) to L0 SSTs, leaving a minimal WAL tail for
// the next opener to replay. It calls the PRODUCTION flush surface -
// (*Slate).Flush, the backend.Flusher capability the cluster's
// displacement flush uses - so the ownerFlush timings measure exactly
// what production pays.
func flushMemtable(t *testing.T, be backend.Backend) time.Duration {
	t.Helper()
	fl := be.(backend.Flusher)
	t0 := time.Now()
	if err := fl.Flush(); err != nil {
		t.Fatalf("openbench: memtable flush: %v", err)
	}
	return time.Since(t0)
}

// abandon force-drops the old owner WITHOUT the clean Shutdown flush,
// modeling the crash / hard-kill shape: the instance stays live (tokio
// background tasks keep running) until the newcomer's open fences it.
// The caller must obReap it after the measured open.
func abandon(be backend.Backend) *Slate { return be.(*Slate) }

// obReap best-effort closes a fenced/abandoned old owner after the
// measurement so tokio tasks don't pile up across iterations. Errors are
// expected (the instance is fenced) and ignored.
func obReap(s *Slate) {
	if s != nil {
		_ = s.Close()
	}
}

// ---------------------------------------------------------------------------
// census: what objects exist at the unit's prefix right before the
// measured open (ground truth for what the open COULD have to read).
// ---------------------------------------------------------------------------

type obCensus struct {
	count map[string]int
	bytes map[string]int64
}

func (c obCensus) total() (n int, b int64) {
	for _, v := range c.count {
		n += v
	}
	for _, v := range c.bytes {
		b += v
	}
	return
}

func (fx *obFixture) census(ru storageunit.ReplicaUnit) obCensus {
	fx.t.Helper()
	prefix := fx.backing.cfg.dbNameReplica(ru) + "/"
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c := obCensus{count: map[string]int{}, bytes: map[string]int64{}}
	for o := range fx.mc.ListObjects(ctx, fx.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if o.Err != nil {
			fx.t.Fatalf("openbench: census list: %v", o.Err)
		}
		cls := obClassify(o.Key, nil)
		c.count[cls]++
		c.bytes[cls] += o.Size
	}
	return c
}

// ---------------------------------------------------------------------------
// measurement + aggregation
// ---------------------------------------------------------------------------

type obTraceSummary struct {
	getManifest, getWal, getSst, list, put, other int
	notFound                                      int // 404 responses (existence probes)
	sumReqTime                                    time.Duration
	respBytes                                     int64
}

func summarizeTrace(stats []obReqStat) obTraceSummary {
	var s obTraceSummary
	for _, r := range stats {
		s.sumReqTime += r.dur
		s.respBytes += r.respBytes
		if r.status == http.StatusNotFound {
			s.notFound++
		}
		switch {
		case r.class == "list":
			s.list++
		case r.method == http.MethodPut || r.method == http.MethodPost:
			s.put++
		case r.class == "manifest":
			s.getManifest++
		case r.class == "wal":
			s.getWal++
		case r.class == "sst":
			s.getSst++
		default:
			s.other++
		}
	}
	return s
}

func (s obTraceSummary) String() string {
	return fmt.Sprintf("GET man=%d wal=%d sst=%d list=%d put=%d other=%d 404s=%d reqTimeSum=%s respBytes=%dKiB",
		s.getManifest, s.getWal, s.getSst, s.list, s.put, s.other, s.notFound,
		s.sumReqTime.Round(time.Millisecond), s.respBytes/1024)
}

type obIterResult struct {
	open        time.Duration
	adminRT     time.Duration // one durable-manifest admin read (the fence read component)
	trace       obTraceSummary
	census      obCensus
	readerBuild time.Duration // L1 scenarios only
	readerWarm  time.Duration // L1 scenarios only
	note        string
}

type obScenarioResult struct {
	name  string
	iters []obIterResult
}

func obMedianDur(ds []time.Duration) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[len(s)/2]
}

func (r obScenarioResult) opens() []time.Duration {
	out := make([]time.Duration, 0, len(r.iters))
	for _, it := range r.iters {
		out = append(out, it.open)
	}
	return out
}

func (r obScenarioResult) minMax() (time.Duration, time.Duration) {
	min, max := time.Duration(1<<62), time.Duration(0)
	for _, it := range r.iters {
		if it.open < min {
			min = it.open
		}
		if it.open > max {
			max = it.open
		}
	}
	if len(r.iters) == 0 {
		return 0, 0
	}
	return min, max
}

// medianIter returns the iteration whose open duration is the median, so
// the reported trace/census belong to a REAL run (not a mix).
func (r obScenarioResult) medianIter() obIterResult {
	if len(r.iters) == 0 {
		return obIterResult{}
	}
	s := append([]obIterResult(nil), r.iters...)
	sort.Slice(s, func(i, j int) bool { return s[i].open < s[j].open })
	return s[len(s)/2]
}

// measuredOpen runs the production fencing open with the proxy trace
// isolated to the open window. Returns the mounted backend so the caller
// can verify and close it. Set SLATE_OPENBENCH_TRACE=1 to dump every
// object-store request of every measured open (offset from open start,
// class, status, latency, bytes, key) - the raw decomposition evidence.
//
// It also times ONE durable-manifest admin read (durableEpochReplica)
// right after the open: that is the same read fenceEpochReplica performs
// INSIDE the measured window, so its cost separates the Go-side fence
// arithmetic component from the opaque DbBuilder.Build remainder.
func (fx *obFixture) measuredOpen(h *Handle, ru storageunit.ReplicaUnit, epoch storageunit.Epoch) (time.Duration, time.Duration, obTraceSummary, backend.Backend) {
	fx.t.Helper()
	fx.proxy.take() // clear anything the shaping phase left behind
	t0 := time.Now()
	be, _, err := h.OpenReplicaUnit(ru, epoch)
	d := time.Since(t0)
	raw := fx.proxy.take()
	trace := summarizeTrace(raw)
	if err != nil {
		fx.t.Fatalf("openbench: measured open of %s: %v", ru, err)
	}
	if os.Getenv("SLATE_OPENBENCH_TRACE") == "1" {
		for _, r := range raw {
			fx.t.Logf("  trace +%6s %-4s %-8s %3d %6s %7dB %s",
				r.start.Sub(t0).Round(time.Millisecond), r.method, r.class, r.status,
				r.dur.Round(time.Millisecond), r.respBytes, r.path)
		}
	}
	ta := time.Now()
	if _, err := fx.backing.durableEpochReplica(ru); err != nil {
		fx.t.Fatalf("openbench: admin read of %s: %v", ru, err)
	}
	adminRT := time.Since(ta)
	return d, adminRT, trace, be
}

func (fx *obFixture) verifyKey(be backend.Backend, i int) {
	fx.t.Helper()
	got, err := be.Get(obKey(i))
	if err != nil {
		fx.t.Fatalf("openbench: post-open verify key %d: %v", i, err)
	}
	if len(got) != obValSize {
		fx.t.Fatalf("openbench: post-open verify key %d: %d bytes, want %d", i, len(got), obValSize)
	}
}

// warmReader is the L1 lever primitive: a non-fencing DbReader open at
// the unit's dbName plus a read pass (spread point gets + a bounded
// prefix scan). Returns a shutdown func the caller invokes AFTER the
// fencing open (the shadow-mount shape keeps the reader alive across the
// flip). A build error is returned, not fatal: reader-vs-live-writer
// feasibility is one of the questions.
func (fx *obFixture) warmReader(ru storageunit.ReplicaUnit, keyCount, maxKey int) (shutdown func(), buildDur, warmDur time.Duration, err error) {
	fx.t.Helper()
	store, err := fx.backing.resolveStore()
	if err != nil {
		return nil, 0, 0, err
	}
	dbName := fx.backing.cfg.dbNameReplica(ru)
	rb := slatedb.NewDbReaderBuilder(dbName, store)
	defer rb.Destroy()
	t0 := time.Now()
	rdr, err := rb.Build()
	buildDur = time.Since(t0)
	if err != nil {
		store.Destroy()
		return nil, buildDur, 0, err
	}

	t1 := time.Now()
	if keyCount > 0 && maxKey > 0 {
		step := maxKey / keyCount
		if step == 0 {
			step = 1
		}
		for i := 0; i < maxKey; i += step {
			_, _ = rdr.Get(obKey(i)) // warm pass; misses are fine
		}
	}
	if it, sErr := rdr.ScanPrefix([]byte("bench:")); sErr == nil {
		for n := 0; n < 1000; n++ {
			kv, nErr := it.Next()
			if nErr != nil || kv == nil {
				break
			}
		}
		it.Destroy()
	}
	warmDur = time.Since(t1)

	shutdown = func() {
		_ = rdr.Shutdown()
		rdr.Destroy()
		store.Destroy()
	}
	return shutdown, buildDur, warmDur, nil
}

// ---------------------------------------------------------------------------
// the benchmark
// ---------------------------------------------------------------------------

// TestOpenBench_FencedOpenWindow runs every scenario and prints one
// summary table. Each scenario iterates obIters() times on a FRESH unit;
// the measured window is always the production Handle.OpenReplicaUnit.
func TestOpenBench_FencedOpenWindow(t *testing.T) {
	fx := newObFixture(t)
	iters := obIters()
	base, tail, seqTail := obBase(), obTail(), obSeqTail()
	t.Logf("openbench: iters=%d base=%d tail=%d seqTail=%d valSize=%dB endpoint=%s (proxied)",
		iters, base, tail, seqTail, obValSize, fx.proxy.url)

	var results []obScenarioResult

	run := func(name string, iter func(i int) obIterResult) {
		res := obScenarioResult{name: name}
		for i := 0; i < iters; i++ {
			ir := iter(i)
			res.iters = append(res.iters, ir)
			t.Logf("%-28s iter %d: open=%s census(n=%d) %s %s",
				name, i, ir.open.Round(time.Millisecond),
				func() int { n, _ := ir.census.total(); return n }(),
				ir.trace.String(), ir.note)
		}
		results = append(results, res)
	}

	// S1 empty-cold: fixed-overhead floor. The unit exists (created +
	// cleanly closed by a prior owner) but holds no data.
	run("S1 empty-cold", func(i int) obIterResult {
		ru := fx.nextUnit()
		hOld, hNew := fx.backing.Handle(), fx.backing.Handle()
		if _, _, err := hOld.OpenReplicaUnit(ru, 1); err != nil {
			t.Fatalf("S1 seed open: %v", err)
		}
		if err := hOld.CloseReplicaUnit(ru); err != nil {
			t.Fatalf("S1 seed close: %v", err)
		}
		census := fx.census(ru)
		d, adminRT, trace, be := fx.measuredOpen(hNew, ru, 2)
		_ = be
		if err := hNew.CloseReplicaUnit(ru); err != nil {
			t.Fatalf("S1 new close: %v", err)
		}
		return obIterResult{open: d, adminRT: adminRT, trace: trace, census: census}
	})

	// S2 clean-flushed: base keys, memtable flushed to L0, clean close.
	// The best-case handed-off unit.
	run("S2 clean-flushed", func(i int) obIterResult {
		ru := fx.nextUnit()
		hOld, hNew := fx.backing.Handle(), fx.backing.Handle()
		be, _, err := hOld.OpenReplicaUnit(ru, 1)
		if err != nil {
			t.Fatalf("S2 seed open: %v", err)
		}
		seedBatched(t, be, 0, base)
		flushMemtable(t, be)
		if err := hOld.CloseReplicaUnit(ru); err != nil {
			t.Fatalf("S2 seed close: %v", err)
		}
		census := fx.census(ru)
		d, adminRT, trace, be2 := fx.measuredOpen(hNew, ru, 2)
		fx.verifyKey(be2, base-1)
		if err := hNew.CloseReplicaUnit(ru); err != nil {
			t.Fatalf("S2 new close: %v", err)
		}
		return obIterResult{open: d, adminRT: adminRT, trace: trace, census: census}
	})

	// dirtyPrep shapes the abrupt-owner unit: flushed base + a dirty
	// unflushed tail written through the production concurrent path, the
	// owner left LIVE (abandoned, not closed). Returns the live old owner.
	dirtyPrep := func(scen string, ru storageunit.ReplicaUnit, hOld *Handle) *Slate {
		be, _, err := hOld.OpenReplicaUnit(ru, 1)
		if err != nil {
			t.Fatalf("%s seed open: %v", scen, err)
		}
		seedBatched(t, be, 0, base)
		flushMemtable(t, be)
		seedConcurrent(t, be, base, tail, obSeedConc)
		return abandon(be)
	}

	// S3 dirty-abandon: the crash / hard-kill shape. The old owner still
	// holds the unit live with an unflushed tail; the measured open
	// fences it and must recover the WAL tail.
	run("S3 dirty-abandon", func(i int) obIterResult {
		ru := fx.nextUnit()
		hOld, hNew := fx.backing.Handle(), fx.backing.Handle()
		old := dirtyPrep("S3", ru, hOld)
		census := fx.census(ru)
		d, adminRT, trace, be2 := fx.measuredOpen(hNew, ru, 2)
		fx.verifyKey(be2, base+tail-1) // tail must have been recovered
		obReap(old)
		if err := hNew.CloseReplicaUnit(ru); err != nil {
			t.Fatalf("S3 new close: %v", err)
		}
		return obIterResult{open: d, adminRT: adminRT, trace: trace, census: census}
	})

	// S4a checkpoint+clean-close (L2, graceful shape): same dirty tail,
	// but the old owner memtable-flushes AND closes cleanly before the
	// newcomer opens.
	run("S4a ckpt+clean-close", func(i int) obIterResult {
		ru := fx.nextUnit()
		hOld, hNew := fx.backing.Handle(), fx.backing.Handle()
		old := dirtyPrep("S4a", ru, hOld)
		fd := flushMemtable(t, old)
		if err := hOld.CloseReplicaUnit(ru); err != nil {
			t.Fatalf("S4a old close: %v", err)
		}
		census := fx.census(ru)
		d, adminRT, trace, be2 := fx.measuredOpen(hNew, ru, 2)
		fx.verifyKey(be2, base+tail-1)
		if err := hNew.CloseReplicaUnit(ru); err != nil {
			t.Fatalf("S4a new close: %v", err)
		}
		return obIterResult{open: d, adminRT: adminRT, trace: trace, census: census,
			note: fmt.Sprintf("ownerFlush=%s", fd.Round(time.Millisecond))}
	})

	// S4b checkpoint+stay-live (L2, overlap-handoff shape): the old owner
	// flushes when it sees the newcomer coming but KEEPS SERVING until
	// fenced; this is the truest model of the flush-on-Joining-bit lever.
	run("S4b ckpt+stay-live", func(i int) obIterResult {
		ru := fx.nextUnit()
		hOld, hNew := fx.backing.Handle(), fx.backing.Handle()
		old := dirtyPrep("S4b", ru, hOld)
		fd := flushMemtable(t, old)
		census := fx.census(ru)
		d, adminRT, trace, be2 := fx.measuredOpen(hNew, ru, 2)
		fx.verifyKey(be2, base+tail-1)
		obReap(old)
		if err := hNew.CloseReplicaUnit(ru); err != nil {
			t.Fatalf("S4b new close: %v", err)
		}
		return obIterResult{open: d, adminRT: adminRT, trace: trace, census: census,
			note: fmt.Sprintf("ownerFlush=%s", fd.Round(time.Millisecond))}
	})

	// S5 reader-warm on the clean unit (L1): a DbReader opens + reads
	// immediately before the fencing open and stays alive across it.
	run("S5 reader-warm clean", func(i int) obIterResult {
		ru := fx.nextUnit()
		hOld, hNew := fx.backing.Handle(), fx.backing.Handle()
		be, _, err := hOld.OpenReplicaUnit(ru, 1)
		if err != nil {
			t.Fatalf("S5 seed open: %v", err)
		}
		seedBatched(t, be, 0, base)
		flushMemtable(t, be)
		if err := hOld.CloseReplicaUnit(ru); err != nil {
			t.Fatalf("S5 seed close: %v", err)
		}
		census := fx.census(ru)
		shutdown, bd, wd, rErr := fx.warmReader(ru, 200, base)
		note := fmt.Sprintf("readerBuild=%s warm=%s", bd.Round(time.Millisecond), wd.Round(time.Millisecond))
		if rErr != nil {
			note = fmt.Sprintf("READER BUILD FAILED: %v", rErr)
		}
		d, adminRT, trace, be2 := fx.measuredOpen(hNew, ru, 2)
		if shutdown != nil {
			shutdown()
		}
		fx.verifyKey(be2, base-1)
		if err := hNew.CloseReplicaUnit(ru); err != nil {
			t.Fatalf("S5 new close: %v", err)
		}
		return obIterResult{open: d, adminRT: adminRT, trace: trace, census: census,
			readerBuild: bd, readerWarm: wd, note: note}
	})

	// S6 reader-warm on the dirty unit (L1 on the crash shape; also the
	// reader-alongside-LIVE-writer feasibility probe, since the abandoned
	// old owner still holds the unit when the reader opens).
	run("S6 reader-warm dirty", func(i int) obIterResult {
		ru := fx.nextUnit()
		hOld, hNew := fx.backing.Handle(), fx.backing.Handle()
		old := dirtyPrep("S6", ru, hOld)
		census := fx.census(ru)
		shutdown, bd, wd, rErr := fx.warmReader(ru, 200, base+tail)
		note := fmt.Sprintf("readerBuild=%s warm=%s (vs LIVE writer)", bd.Round(time.Millisecond), wd.Round(time.Millisecond))
		if rErr != nil {
			note = fmt.Sprintf("READER BUILD vs LIVE WRITER FAILED: %v", rErr)
		}
		d, adminRT, trace, be2 := fx.measuredOpen(hNew, ru, 2)
		if shutdown != nil {
			shutdown()
		}
		fx.verifyKey(be2, base+tail-1)
		obReap(old)
		if err := hNew.CloseReplicaUnit(ru); err != nil {
			t.Fatalf("S6 new close: %v", err)
		}
		return obIterResult{open: d, adminRT: adminRT, trace: trace, census: census,
			readerBuild: bd, readerWarm: wd, note: note}
	})

	// S7 fragmented-clean: same base volume as S2 but memtable-flushed in
	// many small slices -> many small L0/compacted SSTs. If the open cost
	// tracks OBJECT COUNT rather than byte volume, S7 >> S2.
	run("S7 fragmented-clean", func(i int) obIterResult {
		ru := fx.nextUnit()
		hOld, hNew := fx.backing.Handle(), fx.backing.Handle()
		be, _, err := hOld.OpenReplicaUnit(ru, 1)
		if err != nil {
			t.Fatalf("S7 seed open: %v", err)
		}
		const slices = 40
		per := base / slices
		for sl := 0; sl < slices; sl++ {
			seedBatched(t, be, sl*per, per)
			flushMemtable(t, be)
		}
		if err := hOld.CloseReplicaUnit(ru); err != nil {
			t.Fatalf("S7 seed close: %v", err)
		}
		census := fx.census(ru)
		d, adminRT, trace, be2 := fx.measuredOpen(hNew, ru, 2)
		fx.verifyKey(be2, slices*per-1)
		if err := hNew.CloseReplicaUnit(ru); err != nil {
			t.Fatalf("S7 new close: %v", err)
		}
		return obIterResult{open: d, adminRT: adminRT, trace: trace, census: census}
	})

	// S8 wal-fragmented dirty: the tail written one awaited put at a
	// time, so (roughly) one WAL object per write. If WAL replay cost
	// tracks WAL OBJECT COUNT, S8's per-entry open cost >> S3's.
	run("S8 wal-frag dirty", func(i int) obIterResult {
		ru := fx.nextUnit()
		hOld, hNew := fx.backing.Handle(), fx.backing.Handle()
		be, _, err := hOld.OpenReplicaUnit(ru, 1)
		if err != nil {
			t.Fatalf("S8 seed open: %v", err)
		}
		seedBatched(t, be, 0, base)
		flushMemtable(t, be)
		seedSequential(t, be, base, seqTail)
		old := abandon(be)
		census := fx.census(ru)
		d, adminRT, trace, be2 := fx.measuredOpen(hNew, ru, 2)
		fx.verifyKey(be2, base+seqTail-1)
		obReap(old)
		if err := hNew.CloseReplicaUnit(ru); err != nil {
			t.Fatalf("S8 new close: %v", err)
		}
		return obIterResult{open: d, adminRT: adminRT, trace: trace, census: census}
	})

	// ------------------------------------------------------------------
	// summary table
	// ------------------------------------------------------------------
	var sb strings.Builder
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("openbench summary (iters=%d, base=%d, tail=%d, seqTail=%d, val=%dB)\n",
		iters, base, tail, seqTail, obValSize))
	sb.WriteString(fmt.Sprintf("%-24s %10s %10s %10s %9s %9s   %s\n",
		"scenario", "open p50", "min", "max", "gap p50", "admin p50", "median-iter trace + census"))
	for _, r := range results {
		med := obMedianDur(r.opens())
		mn, mx := r.minMax()
		mi := r.medianIter()
		// gap = wall time the median open spent NOT inside an object-store
		// request (FFI, runtime startup, internal waits, and any request
		// pipelining is subtracted twice, so a NEGATIVE gap means requests
		// overlapped, i.e. the open parallelized its I/O).
		gap := mi.open - mi.trace.sumReqTime
		n, b := mi.census.total()
		censusStr := fmt.Sprintf("objs=%d (%dKiB: man=%d wal=%d sst=%d other=%d)",
			n, b/1024, mi.census.count["manifest"], mi.census.count["wal"], mi.census.count["sst"], mi.census.count["other"])
		sb.WriteString(fmt.Sprintf("%-24s %10s %10s %10s %9s %9s   %s | %s",
			r.name, med.Round(time.Millisecond), mn.Round(time.Millisecond), mx.Round(time.Millisecond),
			gap.Round(time.Millisecond), mi.adminRT.Round(time.Millisecond),
			mi.trace.String(), censusStr))
		if mi.readerBuild > 0 {
			sb.WriteString(fmt.Sprintf(" | readerBuild p50=%s", mi.readerBuild.Round(time.Millisecond)))
		}
		sb.WriteString("\n")
	}
	t.Log(sb.String())
}

// TestOpenBench_ReaderProbes answers the standalone L1 feasibility
// questions cheaply (1 unit, no repetition):
//
//   - does a DbReader build against a db whose writer is LIVE (the real
//     shadow-mount precondition)?
//   - how much of the reader's own build is WAL replay
//     (default vs ReaderOptions.SkipWalReplay)?
func TestOpenBench_ReaderProbes(t *testing.T) {
	fx := newObFixture(t)
	base, tail := obBase(), obTail()

	ru := fx.nextUnit()
	hOld := fx.backing.Handle()
	be, _, err := hOld.OpenReplicaUnit(ru, 1)
	if err != nil {
		t.Fatalf("probe seed open: %v", err)
	}
	seedBatched(t, be, 0, base)
	flushMemtable(t, be)
	seedConcurrent(t, be, base, tail, obSeedConc)
	// Old owner stays LIVE for the whole probe.

	// Probe 1: default reader (replays the WAL tail) vs a live writer.
	shutdown, bd, wd, rErr := fx.warmReader(ru, 100, base+tail)
	if rErr != nil {
		t.Logf("PROBE: DbReader build vs LIVE writer FAILED: %v", rErr)
	} else {
		t.Logf("PROBE: DbReader build vs LIVE writer OK: build=%s warm=%s", bd.Round(time.Millisecond), wd.Round(time.Millisecond))
		shutdown()
	}

	// Probe 2: SkipWalReplay reader build time (isolates the reader's own
	// WAL-replay share).
	store, err := fx.backing.resolveStore()
	if err != nil {
		t.Fatalf("probe resolve store: %v", err)
	}
	rb := slatedb.NewDbReaderBuilder(fx.backing.cfg.dbNameReplica(ru), store)
	if err := rb.WithOptions(slatedb.ReaderOptions{
		ManifestPollIntervalMs: 10000,
		CheckpointLifetimeMs:   60000,
		MaxMemtableBytes:       64 << 20,
		SkipWalReplay:          true,
	}); err != nil {
		t.Fatalf("probe reader options: %v", err)
	}
	t0 := time.Now()
	rdr, err := rb.Build()
	skipDur := time.Since(t0)
	rb.Destroy()
	if err != nil {
		t.Logf("PROBE: SkipWalReplay reader build FAILED: %v", err)
		store.Destroy()
	} else {
		t.Logf("PROBE: SkipWalReplay reader build=%s (default-reader build above includes WAL replay)", skipDur.Round(time.Millisecond))
		_ = rdr.Shutdown()
		rdr.Destroy()
		store.Destroy()
	}

	obReap(abandon(be))
	_ = hOld.CloseReplicaUnit(ru)
}
