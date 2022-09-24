package mr

import (
	"log"
)

func init() {
	// Uncomment when debugging
	//log.SetOutput(io.Discard)
	log.SetFlags(log.LstdFlags | log.Llongfile)
}
