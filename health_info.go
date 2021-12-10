package kafka

import (
	"errors"
	"fmt"

	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
)

// HealthInfo contains the health information for one broker
type HealthInfo struct {
	Reachable bool
	HasTopic  bool
}

// HealthInfoMap contains the health information for a set of brokers
type HealthInfoMap map[SaramaBroker]HealthInfo

// UpdateStatus update the provided health.Check state with the current state according
// to the provided minimum number of healthy brokers for the group to be considered healthy.
// If the health status is OK, the provided msgHealthy will be used as status message.
func (h *HealthInfoMap) UpdateStatus(state *health.CheckState, minHealthyThreshold int, msgHealthy string) error {
	if state == nil {
		return errors.New("state in UpdateStatus must not be nil")
	}
	if h == nil || len(*h) == 0 {
		return state.Update(health.StatusCritical, "no brokers defined", 0)
	}

	numHealthy := 0
	for _, healthInfo := range *h {
		if healthInfo.Reachable && healthInfo.HasTopic {
			numHealthy++
		}
	}

	if numHealthy >= minHealthyThreshold {
		if numHealthy == len(*h) {
			// All brokers are healthy
			return state.Update(health.StatusOK, msgHealthy, 0)
		}
		// Enough brokers are healthy, but not all of them
		return state.Update(health.StatusWarning, h.ErrorMsg(), 0)
	}
	// Not enough brokers are healthy
	return state.Update(health.StatusCritical, h.ErrorMsg(), 0)
}

// ErrorMsg returns an tailored message according to the information kept in HealthInfoMap
func (h *HealthInfoMap) ErrorMsg() string {
	errorMsg := ""

	unreachableAddrs := []string{}
	for broker, healthInfo := range *h {
		if !healthInfo.Reachable {
			unreachableAddrs = append(unreachableAddrs, broker.Addr())
		}
	}

	if len(unreachableAddrs) > 0 {
		errorMsg = fmt.Sprintf("broker(s) not reachable at: %v", unreachableAddrs)
	}

	var isReachable = func(addr string) bool {
		for _, u := range unreachableAddrs {
			if u == addr {
				return false
			}
		}
		return true
	}

	noTopicAddrs := []string{}
	for broker, healthInfo := range *h {
		if !healthInfo.HasTopic {
			if isReachable(broker.Addr()) {
				noTopicAddrs = append(noTopicAddrs, broker.Addr())
			}
		}
	}

	if len(noTopicAddrs) > 0 {
		if errorMsg != "" {
			errorMsg += ", "
		}
		errorMsg += fmt.Sprintf("unexpected metadata response from broker(s): %v", noTopicAddrs)
	}

	return errorMsg
}
