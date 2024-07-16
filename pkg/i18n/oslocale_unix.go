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

func getLangFromEnv() (locale string) {
	locale = os.Getenv("LC_ALL")
	if locale == "" {
		locale = os.Getenv("LANG")
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
