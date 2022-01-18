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
	state    State
	mutex    *sync.RWMutex
	channels *ConsumerStateChannels
}

func NewConsumerStateMachine() *StateMachine {
	return &StateMachine{
		state: Initialising,
		mutex: &sync.RWMutex{},
		channels: &ConsumerStateChannels{
			Initialising: NewStateChan(),
			Stopped:      NewStateChan(),
			Starting:     NewStateChan(),
			Consuming:    NewStateChan(),
			Stopping:     NewStateChan(),
			Closing:      NewStateChan(),
		},
	}
}

// Get returns the current state
func (sm *StateMachine) Get() State {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()
	return sm.state
}

// GetChan returns the StateChan pointer corresponding to the provided state
func (sm *StateMachine) GetChan(s State) *StateChan {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()
	return sm.getChan(s)
}

func (sm *StateMachine) getChan(s State) *StateChan {
	switch s {
	case Initialising:
		return sm.channels.Initialising
	case Stopped:
		return sm.channels.Stopped
	case Starting:
		return sm.channels.Starting
	case Consuming:
		return sm.channels.Consuming
	case Stopping:
		return sm.channels.Stopping
	case Closing:
		return sm.channels.Closing
	}
	return nil
}

// String returns the string representation of the current state
func (sm *StateMachine) String() string {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()
	return sm.state.String()
}

// Set sets the state machine to the provided state value
func (sm *StateMachine) Set(newState State) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	sm.transitionTo(newState)
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
	sm.transitionTo(newState)
	return nil
}

// transitionTo leaves the old state (restore the old state channel)
// then sets the new state value
// and then enters to the new state (close the new state channel)
func (sm *StateMachine) transitionTo(newState State) {
	sm.getChan(sm.state).Leave()
	sm.state = newState
	sm.getChan(newState).Enter()
}
