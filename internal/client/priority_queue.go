// Copyright 2023 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import "container/heap"

// Item is the interface that all entries in a priority queue must implement.
type Item interface {
	priority() uint64
	// isCanceled returns true if the item is canceled by the caller.
	isCanceled() bool
}

type PriorityQueue interface {
	Len() int
	Push(item Item)
	Take(n int) []Item
	HighestPriority() uint64
	Reset()
	Clean()
}

var _ PriorityQueue = &HeapQueue{}
var _ PriorityQueue = &ArrayQueue{}

// entry is an entry in a priority queue.
type entry struct {
	entry Item
	index int
}

// prioritySlice implements heap.Interface and holds Entries.
type prioritySlice []entry

// Len returns the length of the priority queue.
func (ps prioritySlice) Len() int {
	return len(ps)
}

// Less compares two entries in the priority queue.
// The higher priority entry is the one with the lower value.
func (ps prioritySlice) Less(i, j int) bool {
	return ps[i].entry.priority() > ps[j].entry.priority()
}

// Swap swaps two entries in the priority queue.
func (ps prioritySlice) Swap(i, j int) {
	ps[i], ps[j] = ps[j], ps[i]
	ps[i].index = i
	ps[j].index = j
}

// Push adds an entry to the priority queue.
func (ps *prioritySlice) Push(x interface{}) {
	item := x.(entry)
	item.index = len(*ps)
	*ps = append(*ps, item)
}

// Pop removes the highest priority entry from the priority queue.
func (ps *prioritySlice) Pop() interface{} {
	old := *ps
	n := len(old)
	item := old[n-1]
	item.index = -1
	*ps = old[0 : n-1]
	return item
}

// HeapQueue is a priority queue.
type HeapQueue struct {
	ps prioritySlice
}

// NewHeapQueue creates a new priority queue.
func NewHeapQueue() *HeapQueue {
	return &HeapQueue{}
}

// Len returns the length of the priority queue.
func (pq *HeapQueue) Len() int {
	return pq.ps.Len()
}

// Push adds an entry to the priority queue.
func (pq *HeapQueue) Push(item Item) {
	heap.Push(&pq.ps, entry{entry: item})
}

// Pop removes the highest priority entry from the priority queue.
func (pq *HeapQueue) Pop() Item {
	if pq.Len() == 0 {
		return nil
	}
	return heap.Pop(&pq.ps).(entry).entry
}

func (aq *HeapQueue) Take(n int) []Item {
	result := make([]Item, 0)
	for i := 0; i < n; i++ {
		item := aq.Pop()
		if item == nil {
			break
		}
		result = append(result, item)
	}
	return result
}

func (pq *HeapQueue) HighestPriority() uint64 {
	if pq.Len() == 0 {
		return 0
	}
	return pq.ps[0].entry.priority()
}

func (pq *HeapQueue) Clean() {
	for i := 0; i < pq.Len(); i++ {
		if pq.ps[i].entry.isCanceled() {
			heap.Remove(&pq.ps, pq.ps[i].index)
		}
	}
}

// Reset clear all entry in the queue.
func (pq *HeapQueue) Reset() {
	for i := 0; i < pq.Len(); i++ {
		pq.ps[i].entry = nil
	}
	pq.ps = pq.ps[:0]
}

// ArrayQueue is a priority queue implemented by array.
type ArrayQueue struct {
	cache [][]Item
	count int
}

func NewArrayQueue() *ArrayQueue {
	cache := make([][]Item, 0)
	return &ArrayQueue{cache: cache}
}

func (aq *ArrayQueue) Len() int {
	return aq.count
}

func (aq *ArrayQueue) Push(item Item) {
	pri := int(item.priority())
	if pri >= len(aq.cache) {
		aq.resize(pri)
	}
	if aq.cache[pri] == nil {
		aq.cache[pri] = make([]Item, 0)
	}
	aq.cache[pri] = append(aq.cache[pri], item)
	aq.count++
}

func (aq *ArrayQueue) Take(n int) []Item {
	high := len(aq.cache) - 1
	result := make([]Item, 0)
	for i := high; i >= 0 && n > 0; i-- {
		list := aq.cache[i]
		acquire := n
		if n > len(list) {
			acquire = len(list)
		}
		n -= acquire
		aq.count -= acquire
		result = append(result, list[:acquire]...)
		aq.cache[i] = aq.cache[i][acquire:]
	}
	return result
}

func (aq *ArrayQueue) HighestPriority() uint64 {
	for i := len(aq.cache) - 1; i >= 0; i-- {
		if len(aq.cache[i]) > 0 {
			return uint64(i)
		}
	}
	return 0
}

func (aq *ArrayQueue) Reset() {
	aq.cache = aq.cache[:0]
}

func (aq *ArrayQueue) resize(max int) {
	cap := max + 1 - len(aq.cache)
	if cap < 0 {
		return
	}
	arr := make([][]Item, cap)
	aq.cache = append(aq.cache, arr...)
}

func (aq *ArrayQueue) Clean() {
	for i := 0; i < len(aq.cache); i++ {
		if aq.cache[i] == nil || len(aq.cache[i]) == 0 {
			continue
		}
		j := 0
		for ; j < len(aq.cache[i]) && aq.cache[i][j].isCanceled(); j++ {
		}
		aq.count -= j
		aq.cache[i] = aq.cache[i][j:]
	}
}
