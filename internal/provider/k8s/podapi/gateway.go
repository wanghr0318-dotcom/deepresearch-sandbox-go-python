package podapi

// GatewayGrant is delivered only to the relay container for one environment.
type GatewayGrant struct {
	URL   string
	CAPEM string
	Token string
}
