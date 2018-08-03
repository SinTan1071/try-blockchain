package common

import (
	"sync"
	"time"
)

const (
	MAX_ALIVETIME = 86400 //最大的缓存时间
)

type LRU interface {
	Start(consumer ...chan []interface{})
	Stop()
	Put(consumer_flag int, key string, lifetime time.Duration, value ...interface{})
	IsAlive(key string) bool
	Remove(key string)
}

//缓存数组
type LRUStruct struct {
	keys     map[string]*data
	list     []keysArray
	stopchan chan struct{}
	time_flag int
	sync.RWMutex
}

type data struct {
	index         int //记录当前的key在list中的位置
	consumer_flag int
	data          interface{}
}

type keysArray []string

func NewLRU() *LRUStruct {
	return &LRUStruct{
		keys:     make(map[string]*data),
		stopchan: make(chan struct{}),
	}
}

//定时任务开启方法，需要传入消费者的管道，并且需要记住对应的consumer的排序，来作为flag来记录
func (l *LRUStruct) Start(consumer ...chan []interface{}) {
	ticker := time.NewTicker(1 * time.Second)
	l.list = make([]keysArray, MAX_ALIVETIME, MAX_ALIVETIME)
	for i := 0; i < MAX_ALIVETIME; i++ {
		l.list[i] = make([]string, 0, 0)
	}
	go func() {
		for {
			select {
			case <-ticker.C:
				keys := l.list[0]
				l.list = append(l.list[1:], make([]string, 0, 0))
				l.time_flag += 1
				go l.remove(keys, consumer...)
			case <-l.stopchan:
				ticker.Stop()
				return
			}
		}
	}()
}

func (l *LRUStruct) Stop() {
	l.stopchan <- struct{}{}
}

//需要把对应的consumer_flag放入数据进行保存，如果对应的需要放入的key-value不存在对应的consumer的话，consumer_flag默认为-1
func (l *LRUStruct) Put(consumer_flag int, key string, lifetime time.Duration, value ...interface{}) {
	index := lifetime / time.Second
	//设置最大保存时间（默认一天）
	if index > MAX_ALIVETIME-1 {
		index = MAX_ALIVETIME - 1
	}
	l.Lock()
	defer l.Unlock()
	l.keys[key] = &data{}
	if len(value) > 0 && len(value) == 1 {
		l.keys[key].index = int(index)
		l.keys[key].consumer_flag = consumer_flag
		l.keys[key].data = value[0]
	}
	l.list[index] = append(l.list[index], key)
}

func (l *LRUStruct) IsAlive(key string) bool {
	l.RLock()
	defer l.RUnlock()
	_, exist := l.keys[key]
	return exist
}

func (l *LRUStruct) Remove(key string) {
	l.Lock()
	defer l.Unlock()
	if value, ok := l.keys[key]; ok {
		keys := l.list[value.index - l.time_flag]
		index, exist := FindStringArrayKeyIndex(keys, key)
		if exist {
			tmp := make([]string, 0, 0)
			tmp = append(tmp, keys[:index]...)
			tmp = append(tmp, keys[index+1:]...)
			l.list[value.index - l.time_flag] = tmp
		}
		delete(l.keys, key)
	}
}

func (l *LRUStruct) remove(keys keysArray, consumer ...chan []interface{}) {
	if len(keys) > 0 {
		l.Lock()
		defer l.Unlock()
		for _, key := range keys {
			if value, ok := l.keys[key]; ok {
				if value.consumer_flag <= len(consumer)-1 && value.consumer_flag >= 0 {
					consumer[value.consumer_flag] <- []interface{}{key, value.data}
				}
				delete(l.keys, key)
			}
		}
	}
}
