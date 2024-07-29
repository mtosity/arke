//go:build windows
// +build windows

package i18n

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	LOCALE_REGEXP = "^[a-z]{2}-[A-Z]{2}$"
)

func TestDetectIETF(t *testing.T) {
	locale, _ := DetectIETF()
	matched, _ := regexp.MatchString(LOCALE_REGEXP, locale)
	assert.True(t, matched)
}
