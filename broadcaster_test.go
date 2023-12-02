package broadcaster

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/kylelemons/godebug/pretty"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestReceiver_Next(t *testing.T) {
	tests := []struct {
		name     string
		timeout  bool
		messages []Message[int] // For simplicity, I'm using int as the generic type T
		want     Message[int]
	}{
		{
			name:     "empty queue",
			timeout:  true,
			messages: []Message[int]{},
			want:     Message[int]{}, // This assumes that a zero value is returned when the queue is empty
		},
		{
			name:     "queue with one message",
			messages: []Message[int]{{data: 10, compiledPath: "path/1", path: []string{"path", "1"}}},
			want:     Message[int]{data: 10, compiledPath: "path/1", path: []string{"path", "1"}},
		},
		{
			name: "queue with multiple messages",
			messages: []Message[int]{
				{data: 10, compiledPath: "path/1", path: []string{"path", "1"}},
				{data: 20, compiledPath: "path/2", path: []string{"path", "2"}},
			},
			want: Message[int]{data: 10, compiledPath: "path/1", path: []string{"path", "1"}}, // First message is expected
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Receiver[int]{
				q: make(chan Message[int], len(tt.messages)),
			}
			for _, msg := range tt.messages {
				r.q <- msg
			}
			ctx := context.Background()
			if tt.timeout {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Duration(1)*time.Millisecond)
				defer cancel()
			}
			got := r.Next(ctx)
			if tt.timeout {
				if !got.IsZero() {
					t.Errorf("Receiver.Next(): expected a zero value message, but got non-zero value")
				}
				return
			}
			if diff := pretty.Compare(tt.want, got); diff != "" {
				t.Errorf("Receiver.Next(): -want/+got:\n%s", diff)
			}
		})
	}
}

func TestReceivers_Add(t *testing.T) {
	receiversList := []*Receiver[int]{
		{q: make(chan Message[int], 1), compiledPath: "1"},
		{q: make(chan Message[int], 1), compiledPath: "2"},
		{q: make(chan Message[int], 1), compiledPath: "3"},
	}

	tests := []struct {
		name             string
		initialReceivers []*Receiver[int] // Using int as the generic type T for simplicity
		newReceiver      *Receiver[int]
		wantLast         *Receiver[int]
	}{
		{
			name:             "add to empty receivers list",
			initialReceivers: []*Receiver[int]{},
			newReceiver:      receiversList[0],
			wantLast:         receiversList[0],
		},
		{
			name:             "add to receivers list with one item",
			initialReceivers: receiversList[:1],
			newReceiver:      receiversList[1],
			wantLast:         receiversList[1],
		},
		{
			name:             "add to receivers list with multiple items",
			initialReceivers: receiversList[:2],
			newReceiver:      receiversList[2],
			wantLast:         receiversList[2],
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetRecvsPtr(receiversList)
			r := &receivers[int]{}

			// Populating the initial list
			for _, receiver := range tt.initialReceivers {
				r.Add(receiver)
			}

			r.Add(tt.newReceiver)

			config := pretty.Config{
				IncludeUnexported: true,
				TrackCycles:       true,
			}

			if diff := config.Compare(tt.wantLast, r.last); diff != "" {
				t.Errorf("After Add(), last receiver in list -want/+got:\n%s", diff)
				t.Logf("Got: %+v", r.last.compiledPath)
				t.Logf("prev: %+v", r.last.prev.compiledPath)
				t.Logf("prev: %+v", r.last.prev.prev.compiledPath)
				t.Logf("first: %+v", r.first.compiledPath)
				t.Logf("first.next: %+v", r.first.next.compiledPath)
				t.Logf("first.next.next: %+v", r.first.next.next.compiledPath)
			}
		})
	}
}

func TestReceivers_Remove(t *testing.T) {
	receiversList := []*Receiver[int]{
		{q: make(chan Message[int], 1), compiledPath: "1"},
		{q: make(chan Message[int], 1), compiledPath: "2"},
		{q: make(chan Message[int], 1), compiledPath: "3"},
	}

	tests := []struct {
		name             string
		initialReceivers []*Receiver[int] // Using int as the generic type T for simplicity
		removeReceiver   *Receiver[int]
		wantFirst        *Receiver[int]
		wantLast         *Receiver[int]
	}{
		{
			name:             "remove from receivers list with one item",
			initialReceivers: receiversList[:1],
			removeReceiver:   receiversList[0],
			wantFirst:        nil,
			wantLast:         nil,
		},
		{
			name:             "remove first receiver from receivers list with multiple items",
			initialReceivers: receiversList[:2],
			removeReceiver:   receiversList[0],
			wantFirst:        receiversList[1],
			wantLast:         receiversList[1],
		},
		{
			name:             "remove middle receiver from receivers list with multiple items",
			initialReceivers: receiversList[:3],
			removeReceiver:   receiversList[1],
			wantFirst:        receiversList[0],
			wantLast:         receiversList[2],
		},
		{
			name:             "remove last receiver from receivers list with multiple items",
			initialReceivers: receiversList[:3],
			removeReceiver:   receiversList[2],
			wantFirst:        receiversList[0],
			wantLast:         receiversList[1],
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetRecvsPtr(receiversList)

			r := &receivers[int]{}

			// Populating the initial list
			for _, receiver := range tt.initialReceivers {
				r.Add(receiver)
			}

			r.Remove(tt.removeReceiver)

			config := pretty.Config{
				IncludeUnexported: true,
				TrackCycles:       true,
			}

			if diff := config.Compare(tt.wantFirst, r.first); diff != "" {
				t.Errorf("After Remove(), first receiver in list -want/+got:\n%s", diff)
				t.Logf("Got: %+v", r.first.compiledPath)
			}

			if diff := config.Compare(tt.wantLast, r.last); diff != "" {
				t.Errorf("After Remove(), last receiver in list -want/+got:\n%s", diff)
				t.Logf("Got: %+v", r.last.compiledPath)

			}
		})
	}
}

func TestReceivers_SendToReceiver(t *testing.T) {
	tests := []struct {
		name          string
		initialQueue  []Message[int]
		newMessage    Message[int]
		expectedQueue []Message[int]
	}{
		{
			name:          "send to receiver with empty queue",
			initialQueue:  []Message[int]{},
			newMessage:    Message[int]{data: 1},
			expectedQueue: []Message[int]{{data: 1}},
		},
		{
			name:          "send to receiver with space in queue",
			initialQueue:  []Message[int]{{data: 1}},
			newMessage:    Message[int]{data: 2},
			expectedQueue: []Message[int]{{data: 1}, {data: 2}},
		},
		{
			name:          "send to receiver with full queue",
			initialQueue:  []Message[int]{{data: 1}, {data: 2}},
			newMessage:    Message[int]{data: 3},
			expectedQueue: []Message[int]{{data: 2}, {data: 3}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Receiver[int]{q: make(chan Message[int], 2)} // Using a queue size of 2 for simplicity
			recvList := &receivers[int]{}

			for _, msg := range tt.initialQueue {
				r.q <- msg
			}

			recvList.sendToReceiver(r, tt.newMessage)

			// Extract messages from the queue to compare
			var actualQueue []Message[int]
			for len(r.q) > 0 {
				actualQueue = append(actualQueue, <-r.q)
			}

			if !reflect.DeepEqual(actualQueue, tt.expectedQueue) {
				t.Errorf("Queue after sendToReceiver = %v, want %v", actualQueue, tt.expectedQueue)
			}
		})
	}
}

func TestReceivers_Send(t *testing.T) {
	tests := []struct {
		name          string
		receiverList  []*Receiver[int]
		messageToSend Message[int]
		expectedData  []int // This represents the data each receiver should have received
	}{
		{
			name:          "send to no receivers",
			receiverList:  []*Receiver[int]{},
			messageToSend: Message[int]{data: 1},
			expectedData:  []int{},
		},
		{
			name:          "send to one receiver",
			receiverList:  []*Receiver[int]{{q: make(chan Message[int], 1)}},
			messageToSend: Message[int]{data: 2},
			expectedData:  []int{2},
		},
		{
			name:          "send to multiple receivers",
			receiverList:  []*Receiver[int]{{q: make(chan Message[int], 1)}, {q: make(chan Message[int], 1)}},
			messageToSend: Message[int]{data: 3},
			expectedData:  []int{3, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recvList := &receivers[int]{}
			for _, receiver := range tt.receiverList {
				if recvList.first == nil {
					recvList.first = receiver
					recvList.last = receiver
				} else {
					recvList.last.next = receiver
					receiver.prev = recvList.last
					recvList.last = receiver
				}
			}

			recvList.Send(tt.messageToSend)

			// Extract data from each receiver's queue to compare
			var actualData []int
			for _, receiver := range tt.receiverList {
				if len(receiver.q) > 0 {
					actualData = append(actualData, (<-receiver.q).data)
				}
			}

			if diff := pretty.Compare(tt.expectedData, actualData); diff != "" {
				t.Errorf("Data received after Send -want/+got:\n%s", diff)
			}
		})
	}
}

func TestSender_Send(t *testing.T) {
	p1s := "path/1"
	p2s := "path/2"
	p1sSub := "path/1/subpath"
	p1 := MkPath(p1s)
	p2 := MkPath(p2s)
	p1Sub := MkPath(p1sSub)

	tests := []struct {
		name           string
		setupReceivers func(s *Sender[int]) []*Receiver[int] // Function to set up the receivers
		messagePath    []string
		messageToSend  Message[int]
		expectedData   map[string]int // Map of receiver paths to expected data
	}{
		{
			name: "send to exact path",
			setupReceivers: func(s *Sender[int]) []*Receiver[int] {
				recvs := []*Receiver[int]{}
				recvs = append(recvs, s.mustReceiver(p1, 1))
				recvs = append(recvs, s.mustReceiver(p2, 1))
				return recvs
			},
			messagePath:   p1,
			messageToSend: Message[int]{data: 10},
			expectedData:  map[string]int{p1s: 10},
		},
		{
			name: "send using wildcard",
			setupReceivers: func(s *Sender[int]) []*Receiver[int] {
				recvs := []*Receiver[int]{}
				recvs = append(recvs, s.mustReceiver(p1, 1))
				recvs = append(recvs, s.mustReceiver(p2, 1))
				recvs = append(recvs, s.mustReceiver(p1Sub, 1))
				return recvs
			},
			messagePath:   MkPath("path/*"),
			messageToSend: Message[int]{data: 20},
			expectedData:  map[string]int{p1s: 20, p2s: 20},
		},
		{
			name: "send using recursive wildcard",
			setupReceivers: func(s *Sender[int]) []*Receiver[int] {
				recvs := []*Receiver[int]{}
				recvs = append(recvs, s.mustReceiver(p1, 1))
				recvs = append(recvs, s.mustReceiver(p2, 1))
				recvs = append(recvs, s.mustReceiver(p1Sub, 1))
				return recvs
			},
			messagePath:   MkPath("path/1/**"),
			messageToSend: Message[int]{data: 30},
			expectedData:  map[string]int{p1s: 30, p1sSub: 30},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log.Println("Running test:", tt.name)
			sender := New[int]()
			recvs := tt.setupReceivers(sender)

			msg, err := NewMessage[int](tt.messagePath, tt.messageToSend.data)
			if err != nil {
				t.Fatal("Failed to create message:", err)
			}

			sender.Send(msg)

			// Check each receiver's queue for the expected data
			for _, recv := range recvs {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				m := recv.Next(ctx)
				cancel()
				if m.IsZero() {
					continue
				}
				want, ok := tt.expectedData[recv.compiledPath]
				if !ok {
					t.Errorf("Receiver at path %s received unexpected data %d", recv.compiledPath, m.data)
					continue
				}
				if m.data != want {
					t.Errorf("Receiver at path %s received %d, want %d", recv.compiledPath, m.data, want)
				}
			}
		})
	}
}

func TestValidatePath(t *testing.T) {
	tests := []struct {
		name  string
		path  []string
		valid bool
	}{
		{
			name:  "empty path",
			path:  []string{},
			valid: false,
		},
		{
			name:  "path with empty string",
			path:  []string{"path", "", "1"},
			valid: false,
		},
		{
			name:  "path with / character",
			path:  []string{"path/1", "path2"},
			valid: false,
		},
		{
			name:  "path with * in middle",
			path:  []string{"path", "*", "1"},
			valid: false,
		},
		{
			name:  "path ending with *",
			path:  []string{"path", "1", "*"},
			valid: true,
		},
		{
			name:  "path ending with **",
			path:  []string{"path", "1", "**"},
			valid: true,
		},
		{
			name:  "path ending with ***",
			path:  []string{"path", "1", "***"},
			valid: false,
		},
		{
			name:  "path ending with abc*",
			path:  []string{"path", "1", "abc*"},
			valid: false,
		},
		{
			name:  "valid path",
			path:  []string{"path", "1", "subpath"},
			valid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePath(tt.path)
			if tt.valid && err != nil {
				t.Errorf("validatePath() for path %v returned unexpected error: %v", tt.path, err)
			} else if !tt.valid && err == nil {
				t.Errorf("validatePath() for path %v expected an error but got none", tt.path)
			}
		})
	}
}

func TestRace(t *testing.T) {
	s := New[int]()
	wg := sync.WaitGroup{}

	for i := 0; i < 10000; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			p := MkPath(fmt.Sprintf("path/%d", i))
			r, err := s.Receiver(p, 1)
			if err != nil {
				panic(err)
			}
			msg, err := NewMessage(p, i)
			if err != nil {
				panic(err)
			}
			if err := s.Send(msg); err != nil {
				panic(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			m := r.Next(ctx)
			if m.IsZero() {
				panic("Received zero value message")
			}
			if m.data != i {
				panic(fmt.Sprintf("Received unexpected data %d", m.data))
			}
			r.Close()
		}()
	}
	wg.Wait()
}

func TestExample1(t *testing.T) {
	s := New[bool]()
	r1, err := s.Receiver(MkPath("foo/yep"), 1)
	if err != nil {
		panic(err)
	}
	r2, err := s.Receiver(MkPath("foo/baz/yep"), 1)
	if err != nil {
		panic(err)
	}
	wg := sync.WaitGroup{}

	got := [2]bool{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		m := r1.Next(context.Background())
		fmt.Println("r1 received: ", m.Data())
		r1.Close()
		got[0] = m.Data()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		m := r2.Next(context.Background())
		fmt.Println("r2 received: ", m.Data())
		r2.Close()
		got[1] = m.Data()
	}()

	// This will send to all receivers, as ** matches all paths recursively.
	// ** is only valid as the last element of a path.
	msg, err := NewMessage(MkPath("**"), true)
	if err != nil {
		panic(err)
	}
	s.Send(msg)

	wg.Wait()

	for i := 0; i < len(got); i++ {
		if !got[i] {
			log.Println("did not receive all expected messages")
		}
	}
}

func TestExample2(t *testing.T) {
	s := New[int]()
	r1, err := s.Receiver(MkPath("foo/yep"), 1)
	if err != nil {
		panic(err)
	}
	r2, err := s.Receiver(MkPath("foo/baz/yep"), 1)
	if err != nil {
		panic(err)
	}
	wg := sync.WaitGroup{}

	got := [2]int{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		m := r1.Next(context.Background())
		fmt.Println("r1 received: ", m.Data())
		r1.Close()
		got[0] = m.Data()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		m := r2.Next(context.Background())
		fmt.Println("r2 received: ", m.Data())
		r2.Close()
		got[1] = m.Data()
	}()

	msg, err := NewMessage(MkPath("foo/yep"), 1)
	if err != nil {
		panic(err)
	}
	s.Send(msg)

	msg, err = NewMessage(MkPath("foo/baz/yep"), 2)
	if err != nil {
		panic(err)
	}
	s.Send(msg)

	wg.Wait()

	for i := 0; i < len(got); i++ {
		if got[i] != i+1 {
			log.Println("did not receive all expected messages")
		}
	}
}

func TestExample3(t *testing.T) {
	s := New[bool]()
	r1, err := s.Receiver(MkPath("foo/bar"), 1)
	if err != nil {
		panic(err)
	}
	r2, err := s.Receiver(MkPath("foo/bar/yep"), 1)
	if err != nil {
		panic(err)
	}
	r3, err := s.Receiver(MkPath("foo/bar/qux/nope"), 1)
	if err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for {
			m := r1.Next(context.Background())
			fmt.Println("r1 received: ", m.Data())
			wg.Done()
		}
	}()

	wg.Add(1)
	go func() {
		for {
			m := r2.Next(context.Background())
			fmt.Println("r2 received: ", m.Data())
			wg.Done()
		}
	}()

	// We don't do a wg.Add() here because this should not get a message.
	go func() {
		for {
			m := r3.Next(context.Background())
			fmt.Println("r3 received: ", m.Data())
		}
	}()

	// This will send to receivers "foo/bar" and "foo/bar/yep".
	// It will not send to "foo/bar/qux/nope" because it is not a direct child of "foo/bar".
	// * is only valid as the last element of a path.
	msg, err := NewMessage(MkPath("foo/bar/*"), true)
	if err != nil {
		panic(err)
	}
	log.Println("here")
	if err := s.Send(msg); err != nil {
		panic(err)
	}
	log.Println("and here")
	wg.Wait()
}

func TestExample4(t *testing.T) {
	s := New[bool]()
	r1, err := s.Receiver(MkPath("foo/bar"), 1)
	if err != nil {
		panic(err)
	}
	r2, err := s.Receiver(MkPath("foo/bar/yep"), 1)
	if err != nil {
		panic(err)
	}
	r3, err := s.Receiver(MkPath("foo/bar/qux/nope"), 1)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			m := r1.Next(context.Background())
			fmt.Println("r1 received: ", m.Data())
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for {
			m := r2.Next(context.Background())
			fmt.Println("r2 received: ", m.Data())
			wg.Done()
		}
	}()

	go func() {
		for {
			m := r3.Next(context.Background())
			fmt.Println("r3 received: ", m.Data())
		}
	}()

	// This will send to receiver "foo/bar/yep" only.
	msg, err := NewMessage(MkPath("foo/bar/yep"), true)
	if err != nil {
		panic(err)
	}
	if err := s.Send(msg); err != nil {
		panic(err)
	}

	wg.Wait()
}

func resetRecvsPtr(recvs []*Receiver[int]) {
	for _, r := range recvs {
		r.next = nil
		r.prev = nil
	}
}
