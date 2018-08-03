package common

import (
	"testing"
	"time"
	"fmt"
)

func TestLRU(t *testing.T) {
	lru := NewLRU()
	consumer := make(chan []interface{})
	fmt.Println("consumer outside 1", consumer)
	go func() {
		for {
			select {
			case values := <- consumer:
				fmt.Println("consumer inside", consumer)
				fmt.Println("key:", values[0], "value:", values[1])
			}
		}
	}()
	lru.Start(consumer)
	fmt.Println("consumer outside 2", consumer)
	lru.Put(-1, "A", 100 * time.Second, "a1")
	lru.Put(0, "B", 5 * time.Second, "b2")
	time.Sleep(20 * time.Second)
	fmt.Println("Is A alive", lru.IsAlive("A"))
	fmt.Println("Is B alive", lru.IsAlive("B"))
	lru.Remove("A")
	//lru.Stop()
}
