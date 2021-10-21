package list

import (
	"errors"
	"fmt"
	"io"
	"strings"

	repl "github.com/brown-csci1270/db/pkg/repl"
)

// List struct.
type List struct {
	head *Link
	tail *Link
}

// Create a new list.
func NewList() *List {
	/* SOLUTION {{{ */
	return &List{head: nil, tail: nil}
	/* SOLUTION }}} */
}

// Get a pointer to the head of the list.
func (list *List) PeekHead() *Link {
	/* SOLUTION {{{ */
	return list.head
	/* SOLUTION }}} */
}

// Get a pointer to the tail of the list.
func (list *List) PeekTail() *Link {
	/* SOLUTION {{{ */
	return list.tail
	/* SOLUTION }}} */
}

// Add an element to the start of the list. Returns the added link.
func (list *List) PushHead(value interface{}) *Link {
	/* SOLUTION {{{ */
	newLink := &Link{
		list:  list,
		next:  list.head,
		value: value,
	}
	if list.tail == nil {
		list.tail = newLink
	}
	if list.head != nil {
		list.head.prev = newLink
	}
	list.head = newLink
	return newLink
	/* SOLUTION }}} */
}

// Add an element to the end of the list. Returns the added link.
func (list *List) PushTail(value interface{}) *Link {
	/* SOLUTION {{{ */
	newLink := &Link{
		list:  list,
		prev:  list.tail,
		value: value,
	}
	if list.head == nil {
		list.head = newLink
	}
	if list.tail != nil {
		list.tail.next = newLink
	}
	list.tail = newLink
	return newLink
	/* SOLUTION }}} */
}

// Find an element in a list given a boolean function, f, that evaluates to true on the desired element.
func (list *List) Find(f func(*Link) bool) *Link {
	/* SOLUTION {{{ */
	for link := list.head; link != nil; {
		if f(link) {
			return link
		}
		if link == list.tail { // Break on last entry
			break
		}
		link = link.next
	}
	return nil
	/* SOLUTION }}} */
}

// Apply a function to every element in the list. f should alter Link in place.
func (list *List) Map(f func(*Link)) {
	/* SOLUTION {{{ */
	for link := list.head; link != nil; {
		f(link)
		if link == list.tail { // Break on last entry
			break
		}
		link = link.next
	}
	/* SOLUTION }}} */
}

// Link struct.
type Link struct {
	list  *List
	prev  *Link
	next  *Link
	value interface{}
}

// Get the list that this link is a part of.
func (link *Link) GetList() *List {
	/* SOLUTION {{{ */
	return link.list
	/* SOLUTION }}} */
}

// Get the link's value.
func (link *Link) GetKey() interface{} {
	/* SOLUTION {{{ */
	return link.value
	/* SOLUTION }}} */
}

// Set the link's value.
func (link *Link) SetKey(value interface{}) {
	/* SOLUTION {{{ */
	link.value = value
	/* SOLUTION }}} */
}

// Get the link's prev.
func (link *Link) GetPrev() *Link {
	/* SOLUTION {{{ */
	return link.prev
	/* SOLUTION }}} */
}

// Get the link's next.
func (link *Link) GetNext() *Link {
	/* SOLUTION {{{ */
	return link.next
	/* SOLUTION }}} */
}

// Remove this link from its list.
func (link *Link) PopSelf() {
	/* SOLUTION {{{ */
	list := link.list
	newPrev := link.prev
	newNext := link.next
	if newPrev != nil {
		newPrev.next = newNext
	}
	if newNext != nil {
		newNext.prev = newPrev
	}
	link.prev = nil
	link.next = nil
	if list.head == link {
		list.head = newNext
	}
	if list.tail == link {
		list.tail = newPrev
	}
	/* SOLUTION }}} */
}

// List REPL.
func ListRepl(list *List) *repl.REPL {
	/* SOLUTION {{{ */
	r := repl.NewRepl()
	r.AddCommand("list_print", func(_ string, replConfig *repl.REPLConfig) error {
		list.Map(func(l *Link) {
			io.WriteString(replConfig.GetWriter(), fmt.Sprintf("%v, ", l.GetKey()))
		})
		return nil
	}, "Prints out the elements of the list. usage: list_print")
	r.AddCommand("list_push_head", func(payload string, replConfig *repl.REPLConfig) error {
		fields := strings.Fields(payload)
		numFields := len(fields)
		if numFields != 2 {
			return errors.New("usage: list_push_head <elt>")
		}
		list.PushHead(fields[1])
		return nil
	}, "Add an element to the head of the list. usage: list_push_head <elt>")
	r.AddCommand("list_push_tail", func(payload string, replConfig *repl.REPLConfig) error {
		fields := strings.Fields(payload)
		numFields := len(fields)
		if numFields != 2 {
			return errors.New("usage: list_push_tail <elt>")
		}
		list.PushTail(fields[1])
		return nil
	}, "Add an element to the tail of the list. usage: list_push_tail <elt>")
	r.AddCommand("list_remove", func(payload string, replConfig *repl.REPLConfig) error {
		fields := strings.Fields(payload)
		numFields := len(fields)
		if numFields != 2 {
			return errors.New("usage: list_push_head <elt>")
		}
		to_remove := list.Find(func(l *Link) bool { return l.GetKey() == fields[1] })
		if to_remove == nil {
			return errors.New("not found")
		}
		to_remove.PopSelf()
		io.WriteString(replConfig.GetWriter(), "removed\n")
		return nil
	}, "Remove an element with the given value from the list. usage: list_remove <elt>")
	r.AddCommand("list_contains", func(payload string, replConfig *repl.REPLConfig) error {
		fields := strings.Fields(payload)
		numFields := len(fields)
		if numFields != 2 {
			return errors.New("usage: list_push_head <elt>")
		}
		found := list.Find(func(l *Link) bool { return l.GetKey() == fields[1] })
		if found != nil {
			io.WriteString(replConfig.GetWriter(), "found!\n")
		} else {
			io.WriteString(replConfig.GetWriter(), "not found\n")
		}
		return nil
	}, "Checks if an element exists in the list. usage: list_contains <elt>")
	return r
	/* SOLUTION }}} */
}
