package main

import (
	"context"

	_ "benthos-prometheus-input/input"

	_ "github.com/benthosdev/benthos/v4/public/components/all"
	"github.com/benthosdev/benthos/v4/public/service"
)

func main() {
	service.RunCLI(context.Background())
}
