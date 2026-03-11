// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package i18n

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

type detectLocaleTestCase struct {
	name     string
	Lang     string
	LcAll    string
	args     []string
	expected string
}

func TestArgsInit(t *testing.T) {
	cases := []detectLocaleTestCase{
		{
			name:     "default",
			Lang:     "",
			LcAll:    "",
			args:     []string{"a"},
			expected: "en",
		},
		{
			name:     "cli args override",
			Lang:     "en-US",
			LcAll:    "",
			args:     []string{"a", "--locale", "zh"},
			expected: "zh",
		},
		{
			name:     EnvLang,
			Lang:     "en-US",
			LcAll:    "",
			args:     []string{"a"},
			expected: "en-US",
		},
		{
			name:     EnvLcAll,
			Lang:     "",
			LcAll:    "de-DE",
			args:     []string{"a"},
			expected: "de-DE",
		},
		{
			name:     EnvLcAll + " priority",
			Lang:     "zh-Hans",
			LcAll:    "de-DE",
			args:     []string{"a"},
			expected: "de-DE",
		},
	}
	for _, c := range cases {
		oldLang := os.Getenv(EnvLang)
		oldLCAll := os.Getenv(EnvLcAll)
		defer func() {
			os.Setenv(EnvLang, oldLang)
			os.Setenv(EnvLcAll, oldLCAll)
		}()
		t.Run(c.name, func(t *testing.T) {
			os.Setenv(EnvLang, c.Lang)
			os.Setenv(EnvLcAll, c.LcAll)
			os.Args = c.args
			l10Messages = nil
			SystemLocale = ""
			InitializeSystemLocale()
			assert.Equal(t, SystemLocale, c.expected, "Incorrect default locale.")
		})
	}
}
