package config

import (
	"log"
	"os"
	"strconv"

	pb "sassoftware.io/convoy/arke/api"
)

func getenv(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return defaultValue
}

// ConnectionConfigurationFromEnv read environment vars and return a ConnectionConfiguration
func ConnectionConfigurationFromEnv() pb.ConnectionConfiguration {
	// needed: SAS_BROKER_HOSTNAME, SAS_BROKER_PORT, SAS_BROKER_USERNAME, SAS_BROKER_PASSWORD
	// SAS_BROKER_TYPE
	hostname := getenv("SAS_BROKER_HOSTNAME", "localhost")
	rawport := getenv("SAS_BROKER_PORT", "5672")
	port, err := strconv.ParseInt(rawport, 10, 32)
	if err != nil {
		log.Fatalf("Could not convert '%s' to int", rawport)
	}
	username := getenv("SAS_BROKER_USERNAME", "")
	password := getenv("SAS_BROKER_PASSWORD", "")
	brokerType := getenv("SAS_BROKER_TYPE", "amqp091")
	tenant := getenv("SAS_BROKER_TENANT", "/")

	caCertificate := []byte(getenv("SAS_BROKER_CA_CERTIFICATE", ""))

	creds := &pb.Credentials{
		Username: username,
		Password: password,
	}

	connConf := pb.ConnectionConfiguration{
		Host:          hostname,
		Port:          int32(port),
		Credentials:   creds,
		Provider:      brokerType,
		Tenant:        tenant,
		CaCertificate: caCertificate,
	}
	return connConf
}
