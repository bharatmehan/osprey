package storage

// ExpiryItem represents an item in the expiry heap
type ExpiryItem struct {
	Key      string
	ExpiryMs int64
	index    int // The index of the item in the heap
}

// ExpiryHeap is a min-heap of expiry items
type ExpiryHeap []*ExpiryItem

func (h ExpiryHeap) Len() int { return len(h) }

func (h ExpiryHeap) Less(i, j int) bool {
	return h[i].ExpiryMs < h[j].ExpiryMs
}

func (h ExpiryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *ExpiryHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*ExpiryItem)
	item.index = n
	*h = append(*h, item)
}

func (h *ExpiryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*h = old[0 : n-1]
	return item
}