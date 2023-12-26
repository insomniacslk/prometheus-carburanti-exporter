package main

import (
	"sync"
	"time"
)

type CacheEntry struct {
	Records []Record
	Ts      time.Time
}

type Cache struct {
	entries map[string]*CacheEntry
	TTL     time.Duration
	mu      sync.Mutex
}

// Get returns the cached item, and a boolean indicating whether the item was found or not.
// If the cached item has expired, a `nil` object and `false` are returned.
func (c *Cache) Get(k string) ([]Record, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[k]
	if ok {
		if time.Since(e.Ts) > c.TTL {
			return nil, false
		}
		return e.Records, true
	}
	return nil, false
}

func (c *Cache) Put(k string, v Record) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[k]
	if ok {
		entry.Records = append(entry.Records, v)
	} else {
		entry = &CacheEntry{
			Records: []Record{v},
			Ts:      time.Now(),
		}
	}
}

func NewCache(ttl time.Duration) *Cache {
	return &Cache{
		entries: make(map[string]*CacheEntry),
		TTL:     ttl,
	}
}
