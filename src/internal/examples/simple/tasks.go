package main

import (
  "fmt"

  "github.com/as3richa/kigo"
)

func init() {
  kigo.RegisterTask("DoNothing", func() error { return nil })
  kigo.RegisterTask("FailAlways", func() error { return fmt.Errorf("augh") })
}
