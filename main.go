package main

import (
	"github.com/WhyIsEmerald/GPDB/internals/db"
)

func main() {
	db, err := db.NewDB[string, string](3, 3)
	db.Put("a", "apple")
	db.Delete("a")
	val, _ := db.Get("a")
}
