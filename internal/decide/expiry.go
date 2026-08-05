package decide

// LeaseRow is one member row as an observation pass read it, reduced to the
// three fields expiry turns on.
type LeaseRow struct {
	// ID is the member's stable identity.
	ID string
	// Inc is the row's INCARNATION nonce: minted fresh whenever the row is
	// CREATED, carried unchanged by every in-place edit. Compared for
	// INEQUALITY only - ordering it would smuggle in a clock.
	Inc string
	// Gen is the member's lease counter, which it advances by renewing.
	Gen uint64
}

// LeaseObservation is what an observer has recorded about one member: the last
// (incarnation, counter) pair it read, and how many consecutive passes that
// pair has sat unchanged.
type LeaseObservation struct {
	Inc   string
	Gen   uint64
	Stale int
}

// ExpireSilent folds one observation pass over the previous observation state
// and returns the state to record plus the members to treat as expired, in row
// order. It is a total function of its arguments: no store, no clock.
//
// LIVENESS IS A RATE THE OBSERVER ITSELF WITNESSES. A member is expired when
// its (incarnation, counter) pair has not moved for expiryPolls CONSECUTIVE
// passes of THIS observer. The comparison is the observer's own pass count
// against the member's renewals, never two wall clocks, so clock skew cannot
// false-expire anyone - and a stalled observer cannot either, because the
// passes it is counting stall with it. Nothing here may consult a clock without
// giving that up.
//
// Four rules, each a way the pair can change or fail to:
//
//   - SELF IS EXEMPT. A node always believes itself alive: its own renewals
//     advance its counter, and an observer that cannot reach the store to renew
//     cannot reach it to poll either. Self is never tracked and never expired,
//     which is also what keeps "the view always contains self" true.
//   - A MEMBER NEW TO THE VIEW STARTS FRESH. Its first appearance is a
//     BASELINE, not evidence of silence: counting it as a stale pass would
//     charge a member for the observer's ignorance of it.
//   - A MOVED COUNTER IS A RENEWAL, A CHANGED INCARNATION IS A NEW ROW. Either
//     resets the count. The incarnation half closes an ABA hole: a member GC'd
//     and rejoining between two passes restarts its counter at 1, which can
//     EQUAL the counter last tracked - judged on the counter alone the observer
//     reads "still not advancing", keeps the member expired, and its GC reaps
//     every fresh row the member writes.
//   - A ROW GONE FROM THE DOCUMENT LEAVES THE STATE. A graceful leave or
//     another observer's GC removes the row; dropping the record means a
//     returning member starts a fresh count instead of inheriting the stale
//     one. This falls out of building the new state from the rows read.
//
// An EXPIRED member stays in the state with its count intact, so a GC that
// loses its CAS race does not restart the member's clock: it stays expired for
// as long as its row sits unchanged.
func ExpireSilent(prev map[string]LeaseObservation, rows []LeaseRow, expiryPolls int, self string) (map[string]LeaseObservation, []string) {
	next := make(map[string]LeaseObservation, len(rows))
	var expired []string
	for _, row := range rows {
		if row.ID == self {
			continue
		}
		obs, tracked := prev[row.ID]
		if !tracked || obs.Inc != row.Inc || obs.Gen != row.Gen {
			next[row.ID] = LeaseObservation{Inc: row.Inc, Gen: row.Gen}
			continue
		}
		obs.Stale++
		next[row.ID] = obs
		if obs.Stale >= expiryPolls {
			expired = append(expired, row.ID)
		}
	}
	return next, expired
}
