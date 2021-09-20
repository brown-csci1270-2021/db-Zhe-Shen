// Main executable for bumblebase.
package main

import (
	"flag"

	config "github.com/brown-csci1270/db/pkg/config"
	list "github.com/brown-csci1270/db/pkg/list"
	"github.com/google/uuid"
)

// Start the database.
func main() {
	lFlag := flag.Bool("list", false, "Whether to import ListREPL")
	cFlag := flag.Bool("c", false, "Whether to print prompt")

	flag.Parse()
	if lFlag != nil && *lFlag {
		myList := list.NewList()
		myREPL := list.ListRepl(myList)
		myREPL.Run(nil, uuid.New(), config.GetPrompt(*cFlag))
	}
}
