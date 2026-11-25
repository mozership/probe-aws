// Command probe-aws serves a Probe action that calls AWS services, and the
// services that speak their protocols: S3, SQS, and any API of AWS sent over
// HTTP, signed with Signature Version 4. Probe starts it for a step that uses
// github.com/mozership/probe-aws@<commit>.
package main

import (
	"github.com/hashicorp/go-hclog"
	"github.com/mozership/probe/sdk"
)

// version is set at build time.
var version = "dev"

func main() {
	sdk.Serve(func(log hclog.Logger) sdk.Action {
		return &Action{log: log}
	})
}
