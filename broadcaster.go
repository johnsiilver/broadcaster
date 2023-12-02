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

	s := New[bool]()
	r1, err := s.Receiver(Path("foo/yep"), 1)
	if err != nil {
		panic(err)
	}
	r2, err := s.Receiver(Path("foo/baz/yep"), 1)
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
	msg, err := NewMessage(Path("**"), true)
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

Example, all receivers for directory foo/bar and any direct child of foo/bar will receive the message:

	s := New[bool]()
	r1, err := s.Receiver(Path("foo/bar"), 1)
	if err != nil {
		panic(err)
	}
	r2, err := s.Receiver(Path("foo/bar/yep"), 1)
	if err != nil {
		panic(err)
	}
	r3, err  := s.Receiver(Path("foo/bar/qux/nope"), 1)
	if err != nil {
		panic(err)
	}

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
	}()

	// This will send to receivers "foo/bar" and "foo/bar/yep".
	// It will not send to "foo/bar/qux/nope" because it is not a direct child of "foo/bar".
	// * is only valid as the last element of a path.
	msg, err := NewMessage(Path("foo/bar/*"), true)
	if err != nil {
		panic(err)
	}

	if err := s.Send(msg); err != nil {
		panic(err)
	}

Example, only the exact path will receive the message:

	s := New[bool]()
	r1, err := s.Receiver(Path("foo/bar"), 1)
	if err != nil {
		panic(err)
	}
	r2, err := s.Receiver(Path("foo/bar/yep"), 1)
	if err != nil {
		panic(err)
	}
	r3, err := s.Receiver(Path("foo/bar/qux/nope"), 1)
	if err != nil {
		panic(err)
	}

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
	}()

	// This will send to receiver "foo/bar/yep" only.
	msg, err := NewMessage(Path("foo/bar/yep"), true)
	if err != nil {
		panic(err)
	}
	if err := s.Send(msg); err != nil {
		panic(err)
	}
*/
package broadcaster

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/derekparker/trie/v2"
)

// Path is a broadcast path as a slice. It roughly looks like a unix file path.
// Create a Path using MkPath.
type Path []string

// MkPath splits a string into a path. It is a convenience function for
// creating paths used in this package from strings. It splits on /.
// A path starting with "/" or ending with "/" will have those trimmed.
func MkPath(s string) Path {
	s = strings.TrimPrefix(s, "/")
	s = strings.TrimSuffix(s, "/")
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
func NewMessage[T any](msgPath Path, data T) (Message[T], error) {
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
		path:         []string(msgPath),
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

// IsZero returns true if the message is the zero value. When receiving with
// a Context that can be cancelled, this must be called to tell if the message
// is usable.
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
}

// Next returns the next message. If you have a context that can be cancelled, you must
// check Message.IsZero() to see if the message is valid.
func (r *Receiver[T]) Next(ctx context.Context) Message[T] {
	select {
	case <-ctx.Done():
		return Message[T]{}
	case m := <-r.q:
		log.Println("Next: I see a returned value: ", m.compiledPath)
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

// Empty returns true if there are no receivers here.
func (r *receivers[T]) Empty() bool {
	if r == nil {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.first == nil
}

// Send sends a message to all receivers that match the path.
func (r *receivers[T]) Send(msg Message[T]) {
	if r == nil {
		return
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	for receiver := r.first; receiver != nil; receiver = receiver.next {
		log.Println("before sendToReceiver")
		r.sendToReceiver(receiver, msg)
		log.Println("after sendToReceiver")
	}
}

func (r *receivers[T]) sendToReceiver(recv *Receiver[T], msg Message[T]) {
sendRecv:
	select {
	// We have space to add to the queue.
	case recv.q <- msg:
		log.Println("receiver got message")
		return
	// There is no space left. We need to make space.
	default:
		// This should yank the latest item off.
		select {
		case <-recv.q:
			log.Println("we did a receiver yank")
		default:
			// This can happen if something grabs the message from recv.q before
			// we can remove it.
		}
		// This should add our new message.
		select {
		case recv.q <- msg:
			log.Println("we added the message the second time")
			return
		default:
			log.Println("looks like we are going to loop")
			goto sendRecv
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
	tree *trie.Trie[*node[T]]
	mu   sync.RWMutex
}

// New creates a new Sender. T is the type of data that will be sent to receivers.
func New[T any]() *Sender[T] {
	return &Sender[T]{
		tree: trie.New[*node[T]](),
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
		paths := s.tree.PrefixSearch(p)
		if len(paths) == 0 {
			return ErrNoMatches
		}
		sent := 0

		reg := regexp.MustCompile(p + "(.*)")
		for _, path := range paths {
			log.Println("I see path: ", path)
			matches := reg.FindStringSubmatch(path)
			if len(matches) != 2 {
				log.Println("yikes")
				continue
			}
			// This means that there is another directory level underneath, which
			// only matches on a recursive search.
			if strings.Contains(matches[1], "/") {
				log.Printf("skipping(%s): %s", matches[1], path)
				continue
			}
			tnode, ok := s.tree.Find(path)
			if !ok {
				log.Println("not finding a prefix we just found: this should never happen")
				continue
			}

			n := tnode.Meta()
			if n.receivers.Empty() {
				log.Println("skipping nil: ", path)
				continue
			}
			log.Println("sending: ", n.path)
			n.receivers.Send(msg)
			sent++
		}
		// The Prefix search doesn't find the exact match for the prefix.
		// So this looks to see if the prefix has an exact match.
		basePath := strings.TrimSuffix(p, "/")
		log.Println("I see path: ", basePath)
		if tnode, ok := s.tree.Find(basePath); ok {
			if !tnode.Meta().receivers.Empty() {
				log.Println("sending: ", tnode.Meta().path)
				tnode.Meta().receivers.Send(msg)
				sent++
			}
		}
		if sent == 0 {
			return ErrNoMatches
		}
		return nil
	// We have **, which means all receivers matching the path and all receivers
	// in that directory and all subdirectories recursively should receive the message.
	case recursWild:
		paths := s.tree.PrefixSearch(p)
		if len(paths) == 0 {
			return ErrNoMatches
		}
		sent := 0
		for _, p := range paths {
			tnode, ok := s.tree.Find(p)
			if !ok {
				log.Println("not finding a prefix we just found: this should never happen")
				continue
			}

			n := tnode.Meta()
			n.receivers.Send(msg)
			sent++
		}

		basePath := strings.TrimSuffix(p, "/")
		if tnode, ok := s.tree.Find(basePath); ok {
			if !tnode.Meta().receivers.Empty() {
				log.Println("sending: ", tnode.Meta().path)
				tnode.Meta().receivers.Send(msg)
				sent++
			}
		}

		if sent == 0 {
			return ErrNoMatches
		}
		return nil
	// We have a normal path, so we just send to receivers for that path.
	default:
		tnode, ok := s.tree.Find(p)
		if !ok {
			return ErrNoMatches
		}

		n := tnode.Meta()
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
func (s *Sender[T]) Receiver(path Path, qSize int) (*Receiver[T], error) {
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
		path:         []string(path),
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
	n := tnode.Meta()
	if n.receivers == nil {
		n.receivers = &receivers[T]{}
	}
	r.receivers = n.receivers
	n.receivers.Add(r)
	return r, nil
}

// mustReceiver is like Receiver, but panics on error. Used in tests
func (s *Sender[T]) mustReceiver(path Path, qSize int) *Receiver[T] {
	r, err := s.Receiver(path, qSize)
	if err != nil {
		panic(err)
	}
	return r
}

func validatePath(p Path) error {
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
