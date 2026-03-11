// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build darwin || freebsd || linux || netbsd || openbsd
// +build darwin freebsd linux netbsd openbsd

package i18n

// from CF jibber_jabber
import (
	"errors"
	"os"
	"strings"

	loc "golang.org/x/text/language"
)

const (
	EnvLcAll = "Lc_ALL"
	EnvLang  = "LANG"
)

func getLangFromEnv() (locale string) {
	locale = os.Getenv(EnvLcAll)
	if locale == "" {
		locale = os.Getenv(EnvLang)
	}
	return
}

func getUnixLocale() (unixLocale string, err error) {
	unixLocale = getLangFromEnv()
	if unixLocale == "" {
		err = errors.New(CouldNotDetectPackageErrorMessage)
	}

	return
}

func DetectIETF() (locale string, err error) {
	unixLocale, err := getUnixLocale()
	if err == nil {
		language, territory := splitLocale(unixLocale)
		locale = language
		if territory != "" {
			locale = strings.Join([]string{language, territory}, "-")
		}
		// Validate the locale value
		_, err = loc.Parse(locale)
		if err != nil {
			locale = ""
		}
	}
	return
}
