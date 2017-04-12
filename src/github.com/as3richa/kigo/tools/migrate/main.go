package main

import (
  "os"
  "fmt"

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

  if err = connection.Migrate(); err != nil {
    fmt.Printf("Couldn't perform migration: %v\n", err)
    os.Exit(1)
  }

  fmt.Println("Migrated")
}
