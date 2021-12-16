package storage

import (
	"sync"
)

type MemDB struct {
	data  map[string]string
	mutex sync.RWMutex
}

func NewMemDB() *MemDB {
	return &MemDB{
		data: make(map[string]string),
	}
}

func (db *MemDB) Get(key string) (string, bool) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	v, ok := db.data[key]
	return v, ok
}

func (db *MemDB) Put(key string, value string) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	db.data[key] = value
}

func (db *MemDB) Delete(key string) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	delete(db.data, key)
}
