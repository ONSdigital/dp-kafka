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

func NewConsumerStateMachine(mutex *sync.RWMutex) *StateMachine {
	return &StateMachine{
		state: Initialising,
		mutex: mutex,
		channels: &ConsumerStateChannels{
			Initialising: make(chan struct{}),
			Stopped:      make(chan struct{}),
			Starting:     make(chan struct{}),
			Consuming:    make(chan struct{}),
			Stopping:     make(chan struct{}),
			Closing:      make(chan struct{}),
		},
	}
}

// Get returns the current state
func (sm *StateMachine) Get() State {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()
	return sm.state
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

// transitionTo restores the old-state channel, sets the new state, and closes the new state channel.
func (sm *StateMachine) transitionTo(newState State) {
	switch sm.state {
	case Initialising:
		sm.channels.Initialising = make(chan struct{})
	case Stopped:
		sm.channels.Stopped = make(chan struct{})
	case Starting:
		sm.channels.Starting = make(chan struct{})
	case Consuming:
		sm.channels.Consuming = make(chan struct{})
	case Stopping:
		sm.channels.Stopping = make(chan struct{})
	case Closing:
		sm.channels.Closing = make(chan struct{})
	}

	sm.state = newState

	switch newState {
	case Initialising:
		SafeClose(sm.channels.Initialising)
	case Stopped:
		SafeClose(sm.channels.Stopped)
	case Starting:
		SafeClose(sm.channels.Starting)
	case Consuming:
		SafeClose(sm.channels.Consuming)
	case Stopping:
		SafeClose(sm.channels.Stopping)
	case Closing:
		SafeClose(sm.channels.Closing)
	}
}
