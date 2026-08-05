// Package decide holds shale's coordination DECISIONS as pure functions:
// given a snapshot of the relevant state, which action follows. Nothing here
// touches a *Cluster, a mutex, a clock, or I/O, and the package imports
// nothing beyond the standard library.
//
// # Why the decisions live apart from the machinery
//
// A decision entangled with the machinery that carries it out can only be
// exercised through that machinery. Testing it then means hand-building a
// &Cluster{} with a timer, a counter and a flag set to what the test BELIEVES
// the reachable states are - so the test encodes the tester's model of
// reachability, which is the very thing under test. A state the model omits is
// a state the table omits, and the omission is invisible: the suite is green
// and the path is unpinned.
//
// That is not hypothetical. The settle-timer arming machine had a test that
// built a LIVE pending timer and never a FIRED one, and the fired-timer path
// was broken: an arm landing there rode an obligation whose callback was
// already in flight, so a single increment got released twice, the pending
// counter went negative, and every later idle-wait waited forever on a
// condition that could no longer hold.
//
// As values, every state is something a table can NAME, so the enumeration is
// visible and its gaps are visible with it. Callers stay thin shells: read the
// state under whatever lock guards it, call the core, perform the returned
// action.
package decide
