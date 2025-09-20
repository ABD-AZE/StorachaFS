// cmd/main.go
package main

import (
	"github.com/ABD-AZE/StorachaFS/cmd/storachafs"
)

// run eg: `./main QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG /tmp/storacha` after building with `go build -o main cmd/main.go`

func main() {
	storachafs.Execute()
}
