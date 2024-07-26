package i18n

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type getLocaleFileTestCase struct {
	filePrefix    string
	locale        string
	expectedFiles []string
}

func TestGetLocaleFiles(t *testing.T) {
	filePrefix := messageFilePrefix
	cases := []getLocaleFileTestCase{
		{
			filePrefix: filePrefix,
			locale:     "en-US",
			expectedFiles: []string{
				fmt.Sprintf("assets/%s_en_US.properties", filePrefix),
				fmt.Sprintf("assets/%s_en.properties", filePrefix),
				fmt.Sprintf("assets/%s.properties", filePrefix),
			},
		},
		{
			filePrefix: filePrefix,
			locale:     "zh-Hans",
			expectedFiles: []string{
				fmt.Sprintf("assets/%s_zh-Hans.properties", filePrefix),
				fmt.Sprintf("assets/%s.properties", filePrefix),
			},
		},
		{
			filePrefix: filePrefix,
			locale:     "zh-SG",
			expectedFiles: []string{
				fmt.Sprintf("assets/%s_zh-Hans.properties", filePrefix),
				fmt.Sprintf("assets/%s.properties", filePrefix),
			},
		},
		{
			filePrefix: filePrefix,
			locale:     "zh-Hant",
			expectedFiles: []string{
				fmt.Sprintf("assets/%s_zh-Hant.properties", filePrefix),
				fmt.Sprintf("assets/%s.properties", filePrefix),
			},
		},
		{
			filePrefix: filePrefix,
			locale:     "zh-MO",
			expectedFiles: []string{
				fmt.Sprintf("assets/%s_zh-Hant.properties", filePrefix),
				fmt.Sprintf("assets/%s.properties", filePrefix),
			},
		},
		{
			filePrefix: filePrefix,
			locale:     "zh-TW",
			expectedFiles: []string{
				fmt.Sprintf("assets/%s_zh-Hant.properties", filePrefix),
				fmt.Sprintf("assets/%s.properties", filePrefix),
			},
		},
		{
			filePrefix: filePrefix,
			locale:     "de-DE",
			expectedFiles: []string{
				fmt.Sprintf("assets/%s_de_DE.properties", filePrefix),
				fmt.Sprintf("assets/%s_de.properties", filePrefix),
				fmt.Sprintf("assets/%s.properties", filePrefix),
			},
		},
	}
	for _, c := range cases {
		t.Run(c.locale, func(t *testing.T) {
			gotFiles := getLocaleFileNames(c.filePrefix, c.locale)
			assert.Equal(t, c.expectedFiles, gotFiles)
		})
	}
}
