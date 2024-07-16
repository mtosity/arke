package i18n

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type getLocaleFileTestCase struct {
	bundleID      string
	locale        string
	expectedFiles []string
}

func TestGetLocaleFiles(t *testing.T) {
	bundleID := ArkeBundleID
	cases := []getLocaleFileTestCase{
		{
			bundleID: bundleID,
			locale:   "en-US",
			expectedFiles: []string{
				fmt.Sprintf("assets/%s_en_US.properties", bundleID),
				fmt.Sprintf("assets/%s_en.properties", bundleID),
				fmt.Sprintf("assets/%s.properties", bundleID),
			},
		},
		{
			bundleID: bundleID,
			locale:   "zh-Hans",
			expectedFiles: []string{
				fmt.Sprintf("assets/%s_zh-Hans.properties", bundleID),
				fmt.Sprintf("assets/%s.properties", bundleID),
			},
		},
		{
			bundleID: bundleID,
			locale:   "zh-SG",
			expectedFiles: []string{
				fmt.Sprintf("assets/%s_zh-Hans.properties", bundleID),
				fmt.Sprintf("assets/%s.properties", bundleID),
			},
		},
		{
			bundleID: bundleID,
			locale:   "zh-Hant",
			expectedFiles: []string{
				fmt.Sprintf("assets/%s_zh-Hant.properties", bundleID),
				fmt.Sprintf("assets/%s.properties", bundleID),
			},
		},
		{
			bundleID: bundleID,
			locale:   "zh-MO",
			expectedFiles: []string{
				fmt.Sprintf("assets/%s_zh-Hant.properties", bundleID),
				fmt.Sprintf("assets/%s.properties", bundleID),
			},
		},
		{
			bundleID: bundleID,
			locale:   "zh-TW",
			expectedFiles: []string{
				fmt.Sprintf("assets/%s_zh-Hant.properties", bundleID),
				fmt.Sprintf("assets/%s.properties", bundleID),
			},
		},
		{
			bundleID: bundleID,
			locale:   "de-DE",
			expectedFiles: []string{
				fmt.Sprintf("assets/%s_de_DE.properties", bundleID),
				fmt.Sprintf("assets/%s_de.properties", bundleID),
				fmt.Sprintf("assets/%s.properties", bundleID),
			},
		},
	}
	for _, c := range cases {
		t.Run(c.locale, func(t *testing.T) {
			gotFiles := getLocaleFileNames(c.bundleID, c.locale)
			assert.Equal(t, c.expectedFiles, gotFiles)
		})
	}
}
