// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build darwin || freebsd || linux || netbsd || openbsd
// +build darwin freebsd linux netbsd openbsd

package i18n

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDetectIETF_LC_ALL(t *testing.T) {
	os.Setenv("LC_ALL", "fr_FR.UTF-8")
	result, _ := DetectIETF()
	assert.Equal(t, result, "fr-FR")
}

func TestDetectIETF_LANG(t *testing.T) {
	os.Setenv("LANG", "fr_FR.UTF-8")
	result, _ := DetectIETF()
	assert.Equal(t, result, "fr-FR")
}

func TestDetectIETF_Blank(t *testing.T) {
	os.Setenv("LC_ALL", "")
	os.Setenv("LANG", "")
	result, _ := DetectIETF()
	assert.Equal(t, result, "")
}

func TestStandardInit(t *testing.T) {
	os.Unsetenv("LANG")
	os.Unsetenv("LC_ALL")
	locale, _ := DetectIETF()
	if "" == locale {
		locale = defaultLanguage
	}
	assert.Equal(t, locale, "en", "Incorrect default locale.")
}

func TestLangC(t *testing.T) {
	os.Unsetenv("LC_ALL")
	os.Setenv("LANG", "C")
	locale, err := DetectIETF()
	assert.NotNil(t, err)
	assert.Equal(t, "", locale)

	os.Setenv("LANG", "C.UTF-8")
	defer os.Unsetenv("LANG")
	locale, err = DetectIETF()
	assert.NotNil(t, err)
	assert.Equal(t, "", locale)
}

func TestInvalidLang(t *testing.T) {
	os.Unsetenv("LC_ALL")
	os.Setenv("LANG", "invalid")
	defer os.Unsetenv("LANG")
	locale, err := DetectIETF()
	assert.NotNil(t, err)
	assert.Equal(t, "", locale)
}
