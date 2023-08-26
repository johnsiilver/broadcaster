/*
Package broadcaster provides a way to broadcast messages to receivers that can then
wake up and take some action based on the contents of the message. Sending and receiving
is done via paths. Paths are unix filesystem like paths where each entry in the path
represents a directory and the end of the path represents a file or directory. Receivers
can subscribe to a path and all messages sent to that path or a path with a wildcard
ending where the wildcard matches the path will be sent to the receiver. If the receiver
has a full queue, the oldest message will be dropped from the queue to make room.
Queue size can be adjusted for all receivers.
It is safe for concurrent use.

Example, all receivers will receive a message:

	s := broadcaster.New[bool]()
	r1 := s.Receiver(broadcaster.Path("foo/yep"), 1)
	r2 := s.Receiver(broadcaster.Path("foo/baz/yep"), 1)

	go func() {
		for {
			m := r1.Next(context.Background())
			fmt.Println("r1 received: ", m.Data())
		}
	}()

	go func() {
		for {
			m := r2.Next(context.Background())
			fmt.Println("r2 received: ", m.Data())
		}
	}()

	// This will send to all receivers, as ** matches all paths recursively.
	// ** is only valid as the last element of a path.
	s.Send(broadcaster.NewMessage(broadcaster.Path("**"), true))

Example, all receivers for directory foo/bar and any direct child of foo/bar will receive the message:

	s := broadcaster.New[bool]()
	r1 := s.Receiver(broadcaster.Path("foo/bar"), 1)
	r2 := s.Receiver(broadcaster.Path("foo/bar/yep"), 1)
	r3 := s.Receiver(broadcaster.Path("foo/bar/qux/nope"), 1)

	go func() {
		for {
			m := r1.Next(context.Background())
			fmt.Println("r1 received: ", m.Data())
		}
	}()

	go func() {
		for {
			m := r2.Next(context.Background())
			fmt.Println("r2 received: ", m.Data())
		}
	}()

	go func() {
		for {
			m := r3.Next(context.Background())
			fmt.Println("r3 received: ", m.Data())
		}
	}

	// This will send to receivers "foo/bar" and "foo/bar/yep".
	// It will not send to "foo/bar/qux/nope" because it is not a direct child of "foo/bar".
	// * is only valid as the last element of a path.
	s.Send(broadcaster.NewMessage(broadcaster.Path("foo/bar/*"), true))

Example, only the exact path will receive the message:

	s := broadcaster.New[bool]()
	r1 := s.Receiver(broadcaster.Path("foo/bar"), 1)
	r2 := s.Receiver(broadcaster.Path("foo/bar/yep"), 1)
	r3 := s.Receiver(broadcaster.Path("foo/bar/qux/nope"), 1)

	go func() {
		for {
			m := r1.Next(context.Background())
			fmt.Println("r1 received: ", m.Data())
		}
	}

	go func() {
		for {
			m := r2.Next(context.Background())
			fmt.Println("r2 received: ", m.Data())
		}
	}

	go func() {
		for {
			m := r3.Next(context.Background())
			fmt.Println("r3 received: ", m.Data())
		}
	}

	// This will send to receiver "foo/bar/yep" only.
	s.Send(broadcaster.NewMessage(broadcaster.Path("foo/bar/yep"), true))
*/
package broadcaster

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/derekparker/trie"
)

// Path splits a string into a path. It is a convenience function for
// creating paths used in this package from strings. It splits on /.
func Path(s string) []string {
	return strings.Split(s, "/")
}

// Message is a message that is sent to a set of receivers.
type Message[T any] struct {
	data         T
	compiledPath string
	path         []string
}

// NewMessage creates a new message for use with a Sender. See details about msgPath
// in Sender.Receiver. data is the data that will be sent with the message where T
// is the type of custom data the Sender will send.
func NewMessage[T any](msgPath []string, data T) (Message[T], error) {
	for _, p := range msgPath {
		if p == "" {
			return Message[T]{}, fmt.Errorf("path cannot contain empty strings")
		}
		if strings.Contains(p, "/") {
			return Message[T]{}, fmt.Errorf("path cannot contain /")
		}
	}
	if err := validatePath(msgPath); err != nil {
		return Message[T]{}, err
	}

	m := Message[T]{
		path:         msgPath,
		compiledPath: strings.Join(msgPath, "/"),
		data:         data,
	}

	return m, nil
}

// Data returns the data of the message.
func (m Message[T]) Data() T {
	return m.data
}

// Path returns the path of the message.
func (m Message[T]) Path() string {
	return m.compiledPath
}

// IsZero returns true if the message is the zero value.
func (m Message[T]) IsZero() bool {
	return reflect.ValueOf(m).IsZero()
}

// Receiver is a receiver of messages.
type Receiver[T any] struct {
	next *Receiver[T]
	prev *Receiver[T]
	q    chan Message[T] // protected by mu
	// The receivers list this Receiver belongs to. This is used to remove
	// the receiver from the list when it is closed.
	receivers    *receivers[T]
	compiledPath string
	path         []string
	mu           sync.RWMutex // protects q
}

// Next returns the next message. If you have a context that can be cancelled, you must
// check Message.IsZero() to see if the message is valid.
func (r *Receiver[T]) Next(ctx context.Context) Message[T] {
	r.mu.RLock()
	defer r.mu.RUnlock()

	select {
	case <-ctx.Done():
		return Message[T]{}
	case m := <-r.q:
		return m
	}
}

// Close closes the receiver. It will remove the receiver from the list of receivers
// it belongs to.
func (r *Receiver[T]) Close() {
	r.receivers.Remove(r)
}

type node[T any] struct {
	receivers *receivers[T]
	path      string
}

// receivers is a list of receivers.
type receivers[T any] struct {
	first *Receiver[T]
	last  *Receiver[T]
	mu    sync.RWMutex // Protects first and last
}

func (r *receivers[T]) Add(receiver *Receiver[T]) {
	r.mu.Lock()
	defer r.mu.Unlock()

	receiver.next = nil
	receiver.prev = nil

	if r.first == nil {
		r.first = receiver
		r.last = receiver
		return
	}

	// Set the current end of the list to point to the new receiver.
	r.last.next = receiver
	receiver.prev = r.last
	// Set the new receiver to be the end of the list.
	r.last = receiver

	// Set the receivers list on the receiver.
	receiver.receivers = r
}

// Remove removes a receiver from the list of receivers.
func (r *receivers[T]) Remove(receiver *Receiver[T]) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if receiver.prev != nil {
		receiver.prev.next = receiver.next
	}
	if receiver.next != nil {
		receiver.next.prev = receiver.prev
	}

	if r.first == receiver {
		r.first = receiver.next
	}
	if r.last == receiver {
		r.last = receiver.prev
	}
}

// Send sends a message to all receivers that match the path.
func (r *receivers[T]) Send(msg Message[T]) {
	if r == nil {
		return
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	for receiver := r.first; receiver != nil; receiver = receiver.next {
		r.sendToReceiver(receiver, msg)
	}
}

func (r *receivers[T]) sendToReceiver(recv *Receiver[T], msg Message[T]) {
	recv.mu.Lock()
	defer recv.mu.Unlock()

	select {
	// We have space to add to the queue.
	case recv.q <- msg:
	// There is no space left. We need to make space.
	default:
		// This should yank the latest item off.
		select {
		case <-recv.q:
		default:
			// We don't allow a queue size of 0.
			panic("this should never happen 1")
		}
		// This should add our new message.
		select {
		case recv.q <- msg:
		default:
			panic("this should never happen 2")
		}
	}
}

// Sender is sends broadcast messages to receivers who subscribe to paths.
// Each path can be an absolute path or a wildcard path that end with /*.
// These should be unix filesystem like paths using / as the separator.
// This is not as smart as a unix filesystem though, so be sensible.
// Receivers are registers with a path and all messages sent to that path or a
// path with a wildcard ending where the wildcard matches the path will be sent to
// the receiver. If the receiver has a full queue, the oldest message will be dropped
// from the queue to make room.
// It is safe for concurrent use.
type Sender[T any] struct {
	tree *trie.Trie
	mu   sync.RWMutex
}

// New creates a new Sender. T is the type of data that will be sent to receivers.
func New[T any]() *Sender[T] {
	return &Sender[T]{
		tree: trie.New(),
	}
}

// ErrNoMatches is returned when no receivers match the path on a Send.
var ErrNoMatches = fmt.Errorf("no matches found for path")

// Send sends a message to all receivers that match the path. If that receiver has
// a full queue, the oldest message will be dropped from the queue to make room.
func (s *Sender[T]) Send(msg Message[T]) error {
	if err := validatePath(msg.path); err != nil {
		return err
	}

	p := msg.compiledPath

	dirWild := false
	recursWild := false
	switch {
	case strings.HasSuffix(p, "**"):
		p = strings.TrimSuffix(p, "**")
		recursWild = true
	case strings.HasSuffix(msg.compiledPath, "*"):
		p = strings.TrimSuffix(p, "*")
		dirWild = true
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	switch {
	// We have *, which means all receivers matching the path and all receivers
	// in that directory should receive the message.
	case dirWild:
		p = strings.TrimSuffix(p, "/")
		log.Println(p)
		tnode, ok := s.tree.Find(p)
		if !ok {
			return ErrNoMatches
		}

		n := tnode.Meta().(*node[T])
		n.receivers.Send(msg)

		for _, child := range tnode.Children() {
			n := child.Meta().(*node[T])
			n.receivers.Send(msg)
		}
		return nil
	// We have **, which means all receivers matching the path and all receivers
	// in that directory and all subdirectories recursively should receive the message.
	case recursWild:
		paths := s.tree.PrefixSearch(p)
		if len(paths) == 0 {
			return ErrNoMatches
		}
		for _, p := range paths {
			tnode, ok := s.tree.Find(p)
			if !ok {
				log.Println("not finding a prefix we just found: this should never happen")
				continue
			}

			n := tnode.Meta().(*node[T])
			n.receivers.Send(msg)
		}
		return nil
	// We have a normal path, so we just send to receivers for that path.
	default:
		tnode, ok := s.tree.Find(p)
		if !ok {
			return ErrNoMatches
		}

		n := tnode.Meta().(*node[T])
		n.receivers.Send(msg)
		return nil
	}
}

// Receiver returns a receiver for the given path. path is a unix filesystem like path
// where each entry in path represents a directory and then end of the path represents
// a file or directory. If the path ends with * or ** it will be treated as a wildcard path.
// A wildcard path ending with * will match all paths that end with the path for one level.
// In a filesystem, this would match all files in that path. A wildcard path ending with
// ** will match all paths that are recursively below the path up to **. So sending to
// [foo, bar, *] will match [foo, bar, baz] but not [foo, bar, baz, qux]. If you send to
// [foo, bar, **] it will match [foo, bar, baz] and [foo, bar, baz, qux].
// the message. qSize is the size of the queue. If qSize is < 1, it will be set to 1.
// If your queue size is too small for the number of messages the receiver is able to process,
// the oldest message will be dropped from the queue to make room, so size appropriately.
func (s *Sender[T]) Receiver(path []string, qSize int) (*Receiver[T], error) {
	if qSize < 1 {
		qSize = 1
	}

	if err := validatePath(path); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Add all the intermediate directories to the tree. This is so that
	// we can do wildcard matches for only file paths in a directory.
	for i := range path {
		p := strings.Join(path[:i+1], "/")
		_, ok := s.tree.Find(p)
		if !ok {
			s.tree.Add(p, &node[T]{path: p})
		}
	}

	r := &Receiver[T]{
		path:         path,
		compiledPath: strings.Join(path, "/"),
		q:            make(chan Message[T], qSize),
	}

	tnode, ok := s.tree.Find(r.compiledPath)
	if !ok {
		receivers := &receivers[T]{}
		receivers.first = r
		n := &node[T]{
			path:      r.compiledPath,
			receivers: receivers,
		}
		s.tree.Add(r.compiledPath, n)
		r.receivers = receivers
		return r, nil
	}
	n := tnode.Meta().(*node[T])
	if n.receivers == nil {
		n.receivers = &receivers[T]{}
	}
	r.receivers = n.receivers
	n.receivers.Add(r)
	return r, nil
}

// mustReceiver is like Receiver, but panics on error. Used in tests
func (s *Sender[T]) mustReceiver(path []string, qSize int) *Receiver[T] {
	r, err := s.Receiver(path, qSize)
	if err != nil {
		panic(err)
	}
	return r
}

func validatePath(p []string) error {
	if len(p) == 0 {
		return fmt.Errorf("path cannot be empty")
	}
	last := len(p) - 1
	for i, s := range p {
		if s == "" {
			return fmt.Errorf("path cannot contain empty strings")
		}
		if strings.Contains(s, "/") {
			return fmt.Errorf("path cannot contain /")
		}
		if i != last {
			if strings.Contains(s, "*") {
				return fmt.Errorf("path cannot contain * except as the final element")
			}
		}
	}
	if strings.Contains(p[last], "*") {
		if utf8.RuneCountInString(p[last]) > 2 {
			return fmt.Errorf("path ending with a * can only be * or **, had %v", p[last])
		}
		for _, r := range p[last] {
			if r != '*' {
				return fmt.Errorf("path ending with a * can only be * or **, had %v", p[last])
			}
		}
	}
	return nil
}
