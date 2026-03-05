// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"embed"
	"os"
	"path"
)

//go:embed integration_test.go
var testFiles embed.FS

func WriteIntegrationTestFile(outDir string) error {
	fn := "integration_test.go"
	err := os.MkdirAll(outDir, 0775)
	if err != nil {
		return err
	}
	outfile := path.Join(outDir, fn)
	contents, err := testFiles.ReadFile(fn)
	if err != nil {
		return err
	}
	return os.WriteFile(outfile, contents, 0644)
}
