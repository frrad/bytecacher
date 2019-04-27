package bytecacher

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
)

type Cacher struct {
	Debug bool

	lookup func(string) ([]byte, error)
	path   string

	locksLock sync.RWMutex
	locks     map[string]*sync.RWMutex
}

func NewCacher(
	lookupFn func(string) ([]byte, error),
	path string,
) *Cacher {
	cacher := &Cacher{
		lookup: lookupFn,
		path:   path,

		locksLock: sync.RWMutex{},
		locks:     map[string]*sync.RWMutex{},
	}

	return cacher
}

func (c *Cacher) debug(fmt string, rest ...interface{}) {
	if !c.Debug {
		return
	}
	log.Printf(fmt, rest...)
}

// getMx gets the mutex for a key, creating it if necessary
func (c *Cacher) getMx(key string) *sync.RWMutex {
	c.locksLock.RLock()
	ans, ok := c.locks[key]
	c.locksLock.RUnlock()

	if ok {
		return ans
	}

	c.locksLock.Lock()
	defer c.locksLock.Unlock()

	ans, ok = c.locks[key]
	if ok {
		return ans
	}

	ans = &sync.RWMutex{}
	c.locks[key] = ans
	return ans
}

func (c *Cacher) Get(key string) ([]byte, error) {
	c.debug("checking for %s in cache", key)
	if ans, err := c.retrieve(key); err == nil {
		c.debug("found!")
		return ans, nil
	}

	return c.store(key)
}

func (c *Cacher) retrieve(key string) ([]byte, error) {
	mx := c.getMx(key)
	mx.RLock()
	defer mx.RUnlock()

	path := c.filePath(key)
	if !fileExists(path) {
		return []byte{}, fmt.Errorf("not stored")
	}

	dat, err := ioutil.ReadFile(path)
	if err != nil {
		return []byte{}, err
	}

	return dat, nil
}

func (c *Cacher) store(key string) ([]byte, error) {
	c.debug("cache miss")
	mx := c.getMx(key)
	mx.Lock()
	defer mx.Unlock()

	ans, err := c.lookup(key)
	if err != nil {
		return []byte{}, err
	}

	path := c.filePath(key)
	err = ioutil.WriteFile(path, ans, 0644)

	return ans, err
}

func (c *Cacher) filePath(key string) string {
	return c.path + "/" + key
}

// fileExists reports if there is a file at the given path or not.
func fileExists(filePath string) bool {

	info, err := os.Stat(filePath)

	if os.IsNotExist(err) {
		return false
	}

	if err == nil {
		return !info.IsDir()
	}

	log.Fatalf("encountered unhandled error %+v while statting file %s", err, filePath)

	// never happens
	return false
}
