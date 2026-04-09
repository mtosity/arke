package provider

import (
	"os"
	"strconv"

	"github.com/sassoftware/arke/internal/util"
)

// Constants and methods relating to scraping provider metrics

// when getting the publish rate, how large of a sample range do you want
const envPublishRateSampleRange = "ARKE_PUBLISH_RATE_SAMPLE_RANGE_SECONDS"
const defaultPublishRateSampleRange = 60

var publishRateSampleRange int

var publishRateSampleInterval int

// when getting the publish rate, what should the sample size be
const envPublishRateSampleInterval = "ARKE_PUBLISH_RATE_SAMPLE_INTERVAL_SECONDS"
const defaultPublishRateSampleInterval = 5

func init() {
	setPublishRateParams()
}

func PublishRateSampleInterval() int {
	return publishRateSampleInterval
}

func PublishRateSampleRange() int {
	return publishRateSampleRange
}

func setPublishRateParams() {
	publishRateSampleInterval = defaultPublishRateSampleInterval
	val := os.Getenv(envPublishRateSampleInterval)
	if val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			publishRateSampleInterval = i
		}
	}

	publishRateSampleRange = defaultPublishRateSampleRange
	val = os.Getenv(envPublishRateSampleRange)
	if val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			publishRateSampleRange = i
		}
	}
	util.Logger.Debugf("Set publish rate sample interval to %d seconds and sample range to %d seconds", publishRateSampleInterval, publishRateSampleRange)
}
