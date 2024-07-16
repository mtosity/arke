package i18n

import (
	"embed"
	"fmt"

	"golang.org/x/text/language"
)

const ArkeBundleID = "arke-log-icu"
const embedPropDir = "assets"

var l10Messages []byte

//go:embed assets/*.properties
var propertyFiles embed.FS

// L10n - Load the localization messages from the appropriate locale properties file
// and return the contents
func L10n() []byte {
	if l10Messages == nil {
		InitializeSystemLocale()
		filePaths := getLocaleFileNames(ArkeBundleID, SystemLocale)
		l10Messages = []byte{}
		for _, p := range filePaths {
			contents, err := propertyFiles.ReadFile(p)
			if err == nil {
				// fmt.Printf("Using property file %s\n", p)
				l10Messages = contents
				break
			}
			// fmt.Printf("Missing property file %s\n", p)
		}
	}
	return l10Messages
}

// getLocaleFileNames - Get the expected locale files in order of precedence for the given bundle
// ID and locale, taking into account special cases for chinese.
func getLocaleFileNames(bundleID string, locale string) []string {
	tag, err := language.Parse(locale)
	if err != nil {
		tag = language.English
	}
	langTmp, _ := tag.Base()
	scriptTmp, _ := tag.Script()
	regionTmp, _ := tag.Region()

	lang := langTmp.String()
	script := scriptTmp.String()
	region := regionTmp.String()

	defaultFile := fmt.Sprintf("%s/%s.properties", embedPropDir, bundleID)

	var files []string

	// Special case for Chinese - must include the script
	if lang == "zh" {
		if script == "Hans" || region == "SG" || (script == "" && region == "") {
			files = append(files, fmt.Sprintf("%s/%s_zh-Hans.properties", embedPropDir, bundleID))
		} else if script == "Hant" || region == "TW" || region == "HK" || region == "MO" {
			files = append(files, fmt.Sprintf("%s/%s_zh-Hant.properties", embedPropDir, bundleID))
		}
		files = append(files, defaultFile)
		return files
	}

	return []string{
		fmt.Sprintf("%s/%s_%s_%s.properties", embedPropDir, bundleID, lang, region),
		fmt.Sprintf("%s/%s_%s.properties", embedPropDir, bundleID, lang),
		defaultFile,
	}
}
