package kafka

import (
	"fmt"
	"sync"
)

type State int

const (
	Initialising State = iota
	Stopped
	Starting
	Consuming
	Stopping
	Closing
)

func (s State) String() string {
	return [...]string{"Initialising", "Stopped", "Starting", "Consuming", "Stopping", "Closing"}[s]
}

type StateMachine struct {
	state State
	mutex *sync.Mutex
}

func NewConsumerStateMachine(st State) *StateMachine {
	return &StateMachine{
		state: st,
		mutex: &sync.Mutex{},
	}
}

// Get returns the current state
func (sm *StateMachine) Get() State {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	return sm.state
}

// String returns the string representation of the current state
func (sm *StateMachine) String() string {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	return sm.state.String()
}

// Set sets the state machine to the provided state value
func (sm *StateMachine) Set(newState State) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	sm.state = newState
}

// SetIf sets the state machine to the provided state value only if the current state is one of the values provided in the list
func (sm *StateMachine) SetIf(allowed []State, newState State) error {
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
		return fmt.Errorf("state transition from %s to %s is not allowed", sm.state, newState)
	}
	sm.state = newState
	return nil
}
