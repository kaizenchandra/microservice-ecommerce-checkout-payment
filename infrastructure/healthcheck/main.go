// A static probe usable in distroless images without adding a shell.
package main

import (
    "fmt"
    "net/http"
    "os"
    "time"
)

func main() {
    if len(os.Args) != 2 { os.Exit(2) }
    client := &http.Client{Timeout: 3 * time.Second}
    response, err := client.Get(os.Args[1])
    if err != nil { fmt.Fprintln(os.Stderr, "health endpoint unavailable"); os.Exit(1) }
    defer response.Body.Close()
    if response.StatusCode < 200 || response.StatusCode >= 300 { os.Exit(1) }
}
