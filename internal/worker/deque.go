package worker

import "sync"

// WorkItem represents a unit of work (an S3 prefix to list).
type WorkItem struct {
	Prefix string
	Depth  int
}

// Deque is a thread-safe double-ended queue for work stealing.
// Uses a ring buffer for O(1) push/pop on both ends.
// Owner pushes/pops from the front (LIFO for locality).
// Thieves steal from the back (FIFO for large chunks).
type Deque struct {
	mu   sync.Mutex
	buf  []WorkItem
	head int // index of first element
	tail int // index one past last element
	len  int
}

func NewDeque() *Deque {
	const initCap = 64
	return &Deque{
		buf: make([]WorkItem, initCap),
	}
}

func (d *Deque) grow() {
	newCap := len(d.buf) * 2
	newBuf := make([]WorkItem, newCap)
	for i := 0; i < d.len; i++ {
		newBuf[i] = d.buf[(d.head+i)%len(d.buf)]
	}
	d.buf = newBuf
	d.head = 0
	d.tail = d.len
}

// PushFront adds work to the front (owner side). O(1) amortized.
func (d *Deque) PushFront(item WorkItem) {
	d.mu.Lock()
	if d.len == len(d.buf) {
		d.grow()
	}
	d.head = (d.head - 1 + len(d.buf)) % len(d.buf)
	d.buf[d.head] = item
	d.len++
	d.mu.Unlock()
}

// PushBatch adds multiple items to the front. O(k) where k = len(items).
func (d *Deque) PushBatch(items []WorkItem) {
	d.mu.Lock()
	for d.len+len(items) > len(d.buf) {
		d.grow()
	}
	// Push in reverse order so items[0] ends up at front
	for i := len(items) - 1; i >= 0; i-- {
		d.head = (d.head - 1 + len(d.buf)) % len(d.buf)
		d.buf[d.head] = items[i]
		d.len++
	}
	d.mu.Unlock()
}

// PopFront takes work from the front (owner side). Returns false if empty.
func (d *Deque) PopFront() (WorkItem, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.len == 0 {
		return WorkItem{}, false
	}
	item := d.buf[d.head]
	d.buf[d.head] = WorkItem{} // clear reference
	d.head = (d.head + 1) % len(d.buf)
	d.len--
	return item, true
}

// StealBack takes up to half the items from the back (thief side).
func (d *Deque) StealBack() ([]WorkItem, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.len == 0 {
		return nil, false
	}
	count := d.len / 2
	if count == 0 {
		count = 1
	}
	stolen := make([]WorkItem, count)
	for i := 0; i < count; i++ {
		idx := (d.tail - count + i + len(d.buf)) % len(d.buf)
		stolen[i] = d.buf[idx]
		d.buf[idx] = WorkItem{} // clear reference
	}
	d.tail = (d.tail - count + len(d.buf)) % len(d.buf)
	d.len -= count
	return stolen, true
}

// Len returns current queue depth.
func (d *Deque) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.len
}
