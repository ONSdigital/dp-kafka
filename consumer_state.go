package kafka

import (
	"fmt"
	"sync"
)

type ConsumerState int

const (
	Initialising ConsumerState = iota
	Stopped
	Starting
	Consuming
	Stopping
	Closing
)

func (s ConsumerState) String() string {
	return [...]string{"Initialising", "Stopped", "Starting", "Consuming", "Stopping", "Closing"}[s]
}

type ConsumerStateMachine struct {
	state ConsumerState
	mutex *sync.Mutex
}

func NewConsumerStateMachine(st ConsumerState) *ConsumerStateMachine {
	return &ConsumerStateMachine{
		state: st,
		mutex: &sync.Mutex{},
	}
}

// Get returns the current state
func (sm *ConsumerStateMachine) Get() ConsumerState {
	sm.mutex.Lock()
	sm.mutex.Unlock()
	return sm.state
}

// String returns the string representation of the current state
func (sm *ConsumerStateMachine) String() string {
	sm.mutex.Lock()
	sm.mutex.Unlock()
	return sm.state.String()
}

// Set sets the state machine to the provided state value
func (sm *ConsumerStateMachine) Set(newState ConsumerState) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	sm.state = newState
}

// SetIf sets the state machine to the provided state value only if the current state is one of the values provided in the list
func (sm *ConsumerStateMachine) SetIf(allowed []ConsumerState, newState ConsumerState) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	transitionAllowed := false
	for _, valid := range allowed {
		if sm.state == valid {
			transitionAllowed = true
			break
		}
	}
	if !transitionAllowed {
		return fmt.Errorf("state transition from %s to %s is not allowed", sm.state.String(), newState.String())
	}
	sm.state = newState
	return nil
}

// SetIfNot sets the state machine to the provided state value only if the current state is NOT one of the values provided in the list
func (sm *ConsumerStateMachine) SetIfNot(forbidden []ConsumerState, newState ConsumerState) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	transitionAllowed := true
	for _, valid := range forbidden {
		if sm.state == valid {
			transitionAllowed = false
			break
		}
	}
	if !transitionAllowed {
		return fmt.Errorf("state transition from %s to %s is not allowed", sm.state.String(), newState.String())
	}
	sm.state = newState
	return nil
}
