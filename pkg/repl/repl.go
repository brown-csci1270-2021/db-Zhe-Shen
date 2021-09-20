package repl

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	uuid "github.com/google/uuid"
)

// REPL struct.
type REPL struct {
	commands map[string]func(string, *REPLConfig) error
	help     map[string]string
}

// REPL Config struct.
type REPLConfig struct {
	writer   io.Writer
	clientId uuid.UUID
}

// Get writer.
func (replConfig *REPLConfig) GetWriter() io.Writer {
	return replConfig.writer
}

// Get address.
func (replConfig *REPLConfig) GetAddr() uuid.UUID {
	return replConfig.clientId
}

// Construct an empty REPL.
func NewRepl() *REPL {
	repl := &REPL{
		commands: make(map[string]func(string, *REPLConfig) error),
		help:     make(map[string]string),
	}
	action := func(string, *REPLConfig) error {
		fmt.Printf(repl.HelpString())
		return nil
	}
	repl.AddCommand(".help", action, "Prints out every command and meta command available to you")
	return repl
}

// Combine a slice of REPLs. If no REPLs are passed in,
// return a NewREPL(). If REPLs have overlapping triggers,
// return an error. Otherwise, return a REPL with the union
// of the triggers.
func CombineRepls(repls []*REPL) (*REPL, error) {
	merged := NewRepl()
	for _, repl := range repls {
		for trigger, action := range repl.commands {
			if _, ok := merged.commands[trigger]; ok {
				return nil, fmt.Errorf("Trigger %v duplicates\n", trigger)
			}
			merged.commands[trigger] = action
			merged.help[trigger] = repl.help[trigger]
		}
	}
	return merged, nil
}

// Get commands.
func (r *REPL) GetCommands() map[string]func(string, *REPLConfig) error {
	return r.commands
}

// Get help.
func (r *REPL) GetHelp() map[string]string {
	return r.help
}

// Add a command, along with its help string, to the set of commands.
func (r *REPL) AddCommand(trigger string, action func(string, *REPLConfig) error, help string) {
	r.commands[trigger] = action
	r.help[trigger] = help
}

// Return all REPL usage information as a string.
func (r *REPL) HelpString() string {
	res := ""
	for trigger, help := range r.help {
		res = res + fmt.Sprintf("%v: %v\n", trigger, help)
	}
	return res
}

// Run the REPL.
func (r *REPL) Run(c net.Conn, clientId uuid.UUID, prompt string) {
	// Get reader and writer; stdin and stdout if no conn.
	var reader io.Reader
	var writer io.Writer
	if c == nil {
		reader = os.Stdin
		writer = os.Stdout
	} else {
		reader = c
		writer = c
	}
	scanner := bufio.NewScanner((reader))
	// replConfig := &REPLConfig{writer: writer, clientId: clientId}
	// Begin the repl loop!
	io.WriteString(writer, prompt)
	for scanner.Scan() {
		text := scanner.Text()
		trigger := cleanInput(text)
		if _, ok := r.commands[trigger]; !ok {
			io.WriteString(writer, "Command not supported!\n")
			io.WriteString(writer, prompt)
			continue
		}
		err := r.commands[trigger](text, &REPLConfig{})
		if err != nil {
			io.WriteString(writer, err.Error())
		}
		io.WriteString(writer, prompt)
	}
}

// cleanInput preprocesses input to the db repl.
func cleanInput(text string) string {
	tokens := strings.Split(text, " ")
	return tokens[0]
}
