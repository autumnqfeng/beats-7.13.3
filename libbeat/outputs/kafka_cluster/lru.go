package kafka_cluster

import (
	"errors"
	"sync"
	"time"

	"container/list"
)

type Lru struct {
	max   int
	l     *list.List
	cache map[interface{}]*list.Element
	mu    *sync.RWMutex
}

type Node struct {
	Key string
	Val *kafkaClient
	// Expiration time, -1: no expire
	Expire int
	// time stamp
	Expires int64
}

func newLruCache(len int) *Lru {
	lru := &Lru{
		max:   len,
		l:     list.New(),
		cache: make(map[interface{}]*list.Element),
		mu:    new(sync.RWMutex),
	}
	go lru.expireTask()
	return lru
}

// expires second
func (l *Lru) add(key string, val *kafkaClient, expire int) error {
	if l.l == nil {
		return errors.New("not init NewLru")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.cache[key]; ok {
		e.Value.(*Node).Val = val
		e.Value.(*Node).Expire = expire
		e.Value.(*Node).Expires = SecondsAdd(SecondsStamp(), expire)
		l.l.MoveToFront(e)
		return nil
	}
	ele := l.l.PushFront(&Node{
		Key:     key,
		Val:     val,
		Expire:  expire,
		Expires: SecondsAdd(SecondsStamp(), expire),
	})
	l.cache[key] = ele
	if l.max != 0 && l.l.Len() > l.max {
		if e := l.l.Back(); e != nil {
			l.l.Remove(e)
			node := e.Value.(*Node)
			delete(l.cache, node.Key)

			// callback
			if node.Val != nil {
				node.Val.Close()
			}
		}
	}
	return nil
}

func (l *Lru) contains(key string) bool {
	_, ok := l.get(key)
	return ok
}

func (l *Lru) get(key string) (val *kafkaClient, ok bool) {
	if l.cache == nil {
		return nil, false
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if ele, ok := l.cache[key]; ok {
		l.l.MoveToFront(ele)
		ele.Value.(*Node).Expires = SecondsAdd(SecondsStamp(), ele.Value.(*Node).Expire)
		return ele.Value.(*Node).Val, true
	}
	return nil, false
}

func (l *Lru) getAll() []*Node {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var data []*Node
	for _, v := range l.cache {
		data = append(data, v.Value.(*Node))
	}
	return data
}

func (l *Lru) del(key string) {
	if l.cache == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if ele, ok := l.cache[key]; ok {
		l.l.Remove(ele)
		delete(l.cache, key)

		// callback
		node := ele.Value.(*Node)
		if node.Val != nil {
			node.Val.Close()
		}
	}
}

func (l *Lru) expireTask() {
	for {
		nodes := l.getAll()
		for _, node := range nodes {
			key := node.Key
			expires := node.Expires
			if node.Expire != -1 &&
				SecondsCompare(SecondsStamp(), expires) {
				l.del(key)
			}
		}
		time.Sleep(2 * time.Second)
	}
}

func SecondsStamp() int64 {
	return time.Now().Unix()
}

func SecondsAdd(secondStamp int64, second int) int64 {
	return secondStamp + int64(second)
}

func SecondsCompare(nowStamp, secondStamp int64) bool {
	return nowStamp > secondStamp
}
