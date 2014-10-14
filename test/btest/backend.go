package btest

import (
	"fmt"
	"github.com/bioothod/elliptics-go/elliptics"
)

func check_backend(ch <-chan *elliptics.DnetBackendsStatus, s *elliptics.Session, addr *elliptics.DnetAddr, backend_id int32, ro bool, delay uint32) error {
	checked := false

	for st := range ch {
		if st.Error != nil {
			return st.Error
		}

		for _, b := range st.Backends {
			if b.Backend == backend_id {
				if b.RO != ro {
					return fmt.Errorf("backend: %d: read-only mismatch: read: %v, must be: %v",
						backend_id, b.RO, ro)
				}
				if b.Delay != delay {
					return fmt.Errorf("backend: %d: delay mismatch: read: %v, must be: %v",
						backend_id, b.Delay, delay)
				}

				checked = true
				break
			}
		}
	}
	if !checked {
		return fmt.Errorf("backend: %d: could not find backend", backend_id)
	}

	checked = false
	for st := range s.BackendsStatus(addr) {
		if st.Error != nil {
			return st.Error
		}

		for _, b := range st.Backends {
			if b.Backend == backend_id {
				if b.RO != ro {
					return fmt.Errorf("backend: %d: check: read-only mismatch: read: %v, must be: %v",
						backend_id, b.RO, ro)
				}
				if b.Delay != delay {
					return fmt.Errorf("backend: %d: check: delay mismatch: read: %v, must be: %v",
						backend_id, b.Delay, delay)
				}

				checked = true
				break
			}
		}
	}
	if !checked {
		return fmt.Errorf("backend: %d: could not find backend", backend_id)
	}

	return nil
}

func TestBackendStatusUpdate(t *BackrunnerTest) error {
	s, err := elliptics.NewSession(t.ell.Node)
	if err != nil {
		return fmt.Errorf("backend-status: could not create elliptics session: %v", err)
	}

	addr, err := elliptics.NewDnetAddrStr(t.elliptics_address[0])
	if err != nil {
		return fmt.Errorf("backend-status: could not create address from '%s': %v", t.elliptics_address[0], err)
	}

	var backend_id int32 = 1

	ch := s.BackendMakeReadOnly(&addr, backend_id)
	err = check_backend(ch, s, &addr, backend_id, true, 0)
	if err != nil {
		return fmt.Errorf("backend-status: make-readonly: %v", err)
	}

	ch = s.BackendMakeWritable(&addr, backend_id)
	err = check_backend(ch, s, &addr, backend_id, false, 0)
	if err != nil {
		return fmt.Errorf("backend-status: make-writable: %v", err)
	}

	var delay uint32 = 1000
	ch = s.BackendSetDelay(&addr, backend_id, delay)
	err = check_backend(ch, s, &addr, backend_id, false, delay)
	if err != nil {
		return fmt.Errorf("backend-status: set-delay: %d: %v", delay, err)
	}

	delay = 0
	ch = s.BackendSetDelay(&addr, backend_id, delay)
	err = check_backend(ch, s, &addr, backend_id, false, delay)
	if err != nil {
		return fmt.Errorf("backend-status: set-delay: %d: %v", delay, err)
	}

	return nil
}
