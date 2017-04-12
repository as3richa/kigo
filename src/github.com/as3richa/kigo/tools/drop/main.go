package main

import (
  "bufio"
  "os"
  "fmt"
  "strings"

  "github.com/as3richa/kigo"
)

func main() {
  var url string
  if len(os.Args) > 1 {
    url = os.Args[1]
  } else {
    url = os.Getenv("KIGO_URL")
  }

  if url == "" {
    fmt.Println("Pass a postgres URL via command-line args or the KIGO_URL environment variable")
    os.Exit(1)
  }

  connection, err := kigo.Connect(url)
  if err != nil {
    fmt.Printf("Couldn't connect to kigo: %v\n", err)
    os.Exit(1)
  }

  reader := bufio.NewReader(os.Stdin)
  fmt.Print("Type `DROP ALL` to confirm > ")

  line, _ := reader.ReadString('\n')
  line = strings.TrimSpace(line)

  if line == "DROP ALL" {
    fmt.Println("Confirmed")
    if err = connection.DropAll(); err != nil {
      fmt.Printf("Couldn't drop: %v\n", err)
      os.Exit(1)
    }
    fmt.Println("Dropped")
  } else {
    fmt.Println("Not confirmed; not doing anything")
  }
}
