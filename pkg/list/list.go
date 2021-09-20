package list

import (
	"fmt"
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
	return &List{head: nil, tail: nil}
}

// Get a pointer to the head of the list.
func (list *List) PeekHead() *Link {
	return list.head
}

// Get a pointer to the tail of the list.
func (list *List) PeekTail() *Link {
	return list.tail
}

// Add an element to the start of the list. Returns the added link.
func (list *List) PushHead(value interface{}) *Link {
	prevHead := list.PeekHead()
	newHead := &Link{
		prev:  nil,
		next:  prevHead,
		value: value,
	}
	list.head = newHead
	if list.tail == nil {
		list.tail = newHead
	} else {
		prevHead.prev = newHead
	}
	newHead.list = list
	return newHead
}

// Add an element to the end of the list. Returns the added link.
func (list *List) PushTail(value interface{}) *Link {
	prevTail := list.PeekTail()
	newTail := &Link{
		prev:  prevTail,
		next:  nil,
		value: value,
	}
	list.tail = newTail
	if list.head == nil {
		list.head = newTail
	} else {
		prevTail.next = newTail
	}
	newTail.list = list
	return newTail
}

// Find an element in a list given a boolean function, f, that evaluates to true on the desired element.
func (list *List) Find(f func(*Link) bool) *Link {
	curr := list.head
	for curr != nil {
		if f(curr) {
			return curr
		}
		curr = curr.next
	}
	return nil
}

// Apply a function to every element in the list. f should alter Link in place.
func (list *List) Map(f func(*Link)) {
	curr := list.head
	for curr != nil {
		f(curr)
		curr = curr.next
	}
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
	return link.list
}

// Get the link's value.
func (link *Link) GetKey() interface{} {
	return link.value
}

// Set the link's value.
func (link *Link) SetKey(value interface{}) {
	link.value = value
}

// Get the link's prev.
func (link *Link) GetPrev() *Link {
	return link.prev
}

// Get the link's next.
func (link *Link) GetNext() *Link {
	return link.next
}

// Remove this link from its list.
func (link *Link) PopSelf() {
	if link.prev != nil {
		link.prev.next = link.next
	} else {
		// link is the head
		link.list.head = link.next
		// link is also tail
		if link.list.tail == link {
			link.list.tail = nil
		}
	}
	if link.next != nil {
		link.next.prev = link.prev
	} else {
		// link is tail
		link.list.tail = link.prev
		// link is also head
		if link.list.head == link {
			link.list.head = nil
		}
	}
}

// List REPL.
func ListRepl(list *List) *repl.REPL {
	listPrint := func(string, *repl.REPLConfig) error {
		res := ""
		curr := list.PeekHead()
		for curr != nil {
			res = res + fmt.Sprintf("%v,", curr.GetKey())
			curr = curr.GetNext()
		}
		if len(res) > 0 {
			res = res[:len(res)-1]
		}
		fmt.Println(res)
		return nil
	}
	listPushHead := func(text string, cfg *repl.REPLConfig) error {
		idx := strings.Index(text, " ")
		if idx == -1 {
			return fmt.Errorf("Usage: list_push_head <elt>\n")
		}
		list = list.PushHead(text[idx+1:]).GetList()
		return nil
	}
	listPushTail := func(text string, cfg *repl.REPLConfig) error {
		idx := strings.Index(text, " ")
		if idx == -1 {
			return fmt.Errorf("Usage: list_push_tail <elt>\n")
		}
		list = list.PushTail(text[idx+1:]).GetList()
		return nil
	}
	listRemove := func(text string, cfg *repl.REPLConfig) error {
		idx := strings.Index(text, " ")
		if idx == -1 {
			return fmt.Errorf("Usage: list_push_tail <elt>\n")
		}
		val := text[idx+1:]
		curr := list.PeekHead()
		for curr != nil {
			if curr.GetKey().(string) == val {
				curr.PopSelf()
				list = curr.list
				return nil
			}
			curr = curr.GetNext()
		}
		return nil
	}
	listContains := func(text string, cfg *repl.REPLConfig) error {
		idx := strings.Index(text, " ")
		if idx == -1 {
			return fmt.Errorf("Usage: list_push_tail <elt>\n")
		}
		val := text[idx+1:]
		curr := list.PeekHead()
		for curr != nil {
			if curr.GetKey().(string) == val {
				fmt.Println("Found!")
				return nil
			}
			curr = curr.GetNext()
		}
		fmt.Println("not found")
		return nil
	}

	rep := repl.NewRepl()
	rep.AddCommand("list_print", listPrint, "Prints out all of the elements in the list in order, separated by commas")
	rep.AddCommand("list_push_head", listPushHead, "Inserts the given element to the List as a string")
	rep.AddCommand("list_push_tail", listPushTail, "Inserts the given element to the end of the List as a string")
	rep.AddCommand("list_remove", listRemove, "Removes the given element from the list")
	rep.AddCommand("list_contains", listContains, "Prints \"found!\" if the element is in the list, prints \"not found\" otherwise")
	return rep
}
