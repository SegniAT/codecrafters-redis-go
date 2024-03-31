package store

import (
	"sync"
	"time"
)

type StoreVal struct {
	Val          string
	StoredAt     time.Time
	ExpiresAfter time.Duration
}

type Store struct {
	store map[string]*StoreVal
	mut   sync.RWMutex
}

func NewStore() *Store {
	return &Store{
		store: make(map[string]*StoreVal),
		mut:   sync.RWMutex{},
	}
}

func (s *Store) Set(key string, val *StoreVal) {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.store[key] = val
}

func (s *Store) Get(key string) (*StoreVal, bool) {
	s.mut.Lock()
	defer s.mut.Unlock()

	val, ok := s.store[key]
	return val, ok
}
