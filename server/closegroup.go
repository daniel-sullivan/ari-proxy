package server

// closeGroup tracks the subscription closers that listen defers, so its
// shutdown check can tell whether every one of them ran. listen calls it from
// one goroutine only.
type closeGroup struct {
	open int
}

// Add registers fn and returns the closer to defer.
func (cg *closeGroup) Add(fn func() error) func() {
	cg.open++
	return func() {
		err := fn()
		if err != nil {
			panic(err.Error())
		}
		cg.open--
	}
}

// Done returns a closed channel once every registered closer has run,
// including when none was registered.
func (cg *closeGroup) Done() <-chan struct{} {
	ch := make(chan struct{})
	if cg.open == 0 {
		close(ch)
	}
	return ch
}
