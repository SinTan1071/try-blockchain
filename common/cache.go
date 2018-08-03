package common

import (
	"container/list"
	"github.com/dgraph-io/badger"
)

//一个通用可排序的缓存设备，在同一个事务中缓存信息进入内存和持久化存储

type Cache struct {
	list list.List
	keys map[string]*list.Element
	db badger.DB
}
