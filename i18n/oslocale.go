package i18n

import (
	"flag"
	"io"
	"os"
	"strings"
)

const (
	CouldNotDetectPackageErrorMessage = "Could not detect language"
	defaultLanguage                   = "en"
)

// SystemLocale - System locale
var SystemLocale = defaultLanguage

func init() {
	InitializeSystemLocale()
}

func InitializeSystemLocale() {
	locale, _ := DetectIETF()
	if "" == locale {
		locale = defaultLanguage
	}
	set := flag.NewFlagSet("i18n", flag.ContinueOnError)
	set.SetOutput(io.Discard)
	set.StringVar(&SystemLocale, "locale", locale, "")
	_ = set.Parse(os.Args[1:])
}

func splitLocale(locale string) (string, string) {
	formattedLocale := strings.Split(locale, ".")[0]
	formattedLocale = strings.ReplaceAll(formattedLocale, "-", "_")

	pieces := strings.Split(formattedLocale, "_")
	language := pieces[0]
	territory := ""
	if len(pieces) > 1 {
		territory = strings.Split(formattedLocale, "_")[1]
	}
	return language, territory
}
