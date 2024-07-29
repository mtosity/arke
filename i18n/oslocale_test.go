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
			name:     "LANG",
			Lang:     "en-US",
			LcAll:    "",
			args:     []string{"a"},
			expected: "en-US",
		},
		{
			name:     "LC_ALL",
			Lang:     "",
			LcAll:    "de-DE",
			args:     []string{"a"},
			expected: "de-DE",
		},
		{
			name:     "LC_ALL priority",
			Lang:     "zh-Hans",
			LcAll:    "de-DE",
			args:     []string{"a"},
			expected: "de-DE",
		},
	}
	for _, c := range cases {
		oldLang := os.Getenv("LANG")
		oldLCAll := os.Getenv("LC_ALL")
		defer func() {
			os.Setenv("LANG", oldLang)
			os.Setenv("LC_ALL", oldLCAll)
		}()
		t.Run(c.name, func(t *testing.T) {
			os.Setenv("LANG", c.Lang)
			os.Setenv("LC_ALL", c.LcAll)
			os.Args = c.args
			l10Messages = nil
			SystemLocale = ""
			InitializeSystemLocale()
			assert.Equal(t, SystemLocale, c.expected, "Incorrect default locale.")
		})
	}
}
