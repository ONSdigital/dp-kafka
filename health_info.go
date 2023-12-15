package kafka

import (
	"errors"
	"fmt"

	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/dp-kafka/v4/interfaces"
)

// HealthInfo contains the health information for one broker
type HealthInfo struct {
	Reachable bool
	HasTopic  bool
}

// HealthInfoMap contains the health information for a set of brokers
// with a common topic expected to be available in all of them
type HealthInfoMap struct {
	topic   string
	infoMap map[interfaces.SaramaBroker]HealthInfo
}

// Set creates or overrides a HealthInfo value for the provided broker
func (h *HealthInfoMap) Set(broker interfaces.SaramaBroker, healthInfo HealthInfo) {
	if h.infoMap == nil {
		h.infoMap = map[interfaces.SaramaBroker]HealthInfo{}
	}
	h.infoMap[broker] = healthInfo
}

// UpdateStatus update the provided health.Check state with the current state according
// to the provided minimum number of healthy brokers for the group to be considered healthy.
// If the health status is OK, the provided msgHealthy will be used as status message.
func (h *HealthInfoMap) UpdateStatus(state *health.CheckState, minHealthyThreshold int, msgHealthy string) error {
	if state == nil {
		return errors.New("state in UpdateStatus must not be nil")
	}
	if h == nil || len(h.infoMap) == 0 {
		return state.Update(health.StatusCritical, "no brokers defined", 0)
	}

	numHealthy := 0
	for _, healthInfo := range h.infoMap {
		if healthInfo.Reachable && healthInfo.HasTopic {
			numHealthy++
		}
	}

	if numHealthy == len(h.infoMap) {
		// All brokers are healthy
		return state.Update(health.StatusOK, msgHealthy, 0)
	}

	if numHealthy >= minHealthyThreshold {
		// Enough brokers are healthy, but not all of them.  We should still return OK though as the services should not fail at this point
		return state.Update(health.StatusOK, h.errorMsg(), 0)
	}

	// Not enough brokers are healthy
	return state.Update(health.StatusCritical, h.errorMsg(), 0)
}

// errorMsg returns an tailored message according to the information kept in HealthInfoMap
func (h *HealthInfoMap) errorMsg() string {
	errorMsg := ""

	// Check for unhealthy brokers and add as list to error message if present
	unreachableAddrs := []string{}
	for broker, healthInfo := range h.infoMap {
		if !healthInfo.Reachable {
			unreachableAddrs = append(unreachableAddrs, broker.Addr())
		}
	}

	if len(unreachableAddrs) > 0 {
		errorMsg = fmt.Sprintf("broker(s) not reachable at: %v", unreachableAddrs)
	}

	// Check for brokers that are missing the topic and add as list to error message if present
	noTopicAddrs := []string{}
	for broker, healthInfo := range h.infoMap {
		if healthInfo.Reachable && !healthInfo.HasTopic {
			noTopicAddrs = append(noTopicAddrs, broker.Addr())
		}
	}

	if len(noTopicAddrs) > 0 {
		if errorMsg != "" {
			errorMsg += ", "
		}
		errorMsg += fmt.Sprintf("topic %s not available in broker(s): %v", h.topic, noTopicAddrs)
	}

	return errorMsg
}
