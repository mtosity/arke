// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package i18n

import (
	"bufio"
	"bytes"
	"embed"
	"fmt"
	"strings"
	"sync"

	"golang.org/x/text/language"
)

const embedPropDir = "resources"
const messageFilePrefix = "GoLogMessages"

var (
	l10Messages    []byte
	l10SyncOnce    = sync.Once{}
	bundle         *propertiesBundle
	bundleSyncOnce = sync.Once{}
	bundleErr      error
)

//go:embed resources/*.properties
var propertyFiles embed.FS

func init() {
	// Force initialization of system locale at startup
	_, _ = newPropertiesBundle(L10n())
}

// L10n - Load the localization messages from the appropriate locale properties file
// and return the contents
func L10n() []byte {
	l10SyncOnce.Do(func() {
		InitializeSystemLocale()
		filePaths := getLocaleFileNames(messageFilePrefix, SystemLocale)
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
	})
	return l10Messages
}

// getLocaleFileNames - Get the expected locale files in order of precedence for the given bundle
// ID and locale, taking into account special cases for chinese.
func getLocaleFileNames(filePrefix string, locale string) []string {
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

	defaultFile := fmt.Sprintf("%s/%s.properties", embedPropDir, filePrefix)

	var files []string

	// Special case for Chinese - must include the script
	if lang == "zh" {
		if script == "Hans" || region == "SG" || (script == "" && region == "") {
			files = append(files, fmt.Sprintf("%s/%s_zh-Hans.properties", embedPropDir, filePrefix))
		} else if script == "Hant" || region == "TW" || region == "HK" || region == "MO" {
			files = append(files, fmt.Sprintf("%s/%s_zh-Hant.properties", embedPropDir, filePrefix))
		}
		files = append(files, defaultFile)
		return files
	}

	return []string{
		fmt.Sprintf("%s/%s_%s_%s.properties", embedPropDir, filePrefix, lang, region),
		fmt.Sprintf("%s/%s_%s.properties", embedPropDir, filePrefix, lang),
		defaultFile,
	}
}

// propertiesBundle holds message templates loaded from .properties files
type propertiesBundle struct {
	messages map[string]string
}

// newPropertiesBundle creates a new properties bundle and parses the given data
func newPropertiesBundle(data []byte) (*propertiesBundle, error) {
	bundleSyncOnce.Do(func() {
		bundle = &propertiesBundle{
			messages: make(map[string]string),
		}

		// If there's no data to load, we still do not want to return
		// and error - we will just log the message keys
		if data == nil {
			return
		}
		scanner := bufio.NewScanner(bytes.NewReader(data))

		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())

			// Skip comments and empty lines
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}

			// Parse key=value
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				key := strings.TrimSpace(parts[0])
				value := strings.TrimSpace(parts[1])
				bundle.messages[key] = value
			}
		}
		if scanner.Err() != nil {
			bundleErr = scanner.Err()
			fmt.Printf("Error reading properties data: %v\n", bundleErr)
		}
	})

	return bundle, bundleErr
}

// T - Translate the given message ID to the localized message and substitute
// parameters. If the message ID is not found in the bundle, the message ID itself is
// used as the message template, parameters substituted, and returned.
func T(messageID string, args ...interface{}) string {
	msg := messageID
	if msgTemplate, ok := bundle.messages[messageID]; ok {
		msg = msgTemplate
	}
	for i, arg := range args {
		placeholder := fmt.Sprintf("{%d}", i)
		msg = strings.ReplaceAll(msg, placeholder, fmt.Sprintf("%v", arg))
	}
	return msg
}
