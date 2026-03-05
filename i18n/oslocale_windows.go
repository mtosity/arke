// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build windows
// +build windows

package i18n

// from CF jibber_jabber
import (
	"fmt"
	"os/exec"
	"strings"
)

// DetectIETF returns the os locale
func DetectIETF() (locale string, err error) {
	out, err := exec.Command("powershell", "Get-Culture | select -exp Name").Output()
	if err != nil {
		return "", err
	}
	locale = strings.TrimSpace(strings.ReplaceAll(string(out), "\r\n", ""))
	if locale == "" {
		return "", fmt.Errorf("failed to get the OS locale")
	}
	return
}
