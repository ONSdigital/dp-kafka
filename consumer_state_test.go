package kafka

import (
	"fmt"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSet(t *testing.T) {
	Convey("Given a consumer-group state machine", t, func() {
		sm := NewConsumerStateMachine(&sync.RWMutex{})

		validateTransition(sm, Stopped, sm.channels.Stopped, sm.channels.Initialising)
		validateTransition(sm, Starting, sm.channels.Starting, sm.channels.Stopped)
		validateTransition(sm, Consuming, sm.channels.Consuming, sm.channels.Starting)
		validateTransition(sm, Stopping, sm.channels.Stopping, sm.channels.Consuming)
		validateTransition(sm, Closing, sm.channels.Closing, sm.channels.Stopping)
	})
}

func TestSetIf(t *testing.T) {
	Convey("Given a consumer-group state machine in Stopped state, with the Stopped channel closed", t, func(c C) {
		sm := NewConsumerStateMachine(&sync.RWMutex{})
		sm.Set(Stopped)
		validateChanClosed(c, sm.channels.Stopped.channel, true)

		Convey("Then calling SetIf with an initial state condition that does not contain Stopped state results in the state not being changed and the channels remaining the same", func(c C) {
			sm.SetIf([]State{Consuming}, Starting)
			So(sm.state, ShouldEqual, Stopped)
			validateChanClosed(c, sm.channels.Stopped.channel, true)
			validateChanClosed(c, sm.channels.Starting.channel, false)
		})

		Convey("Then calling SetIf with an initial state condition that does contain Stopped state results in the state being changed and the channels being closed and created accordingly", func(c C) {
			sm.SetIf([]State{Consuming, Stopped}, Starting)
			So(sm.state, ShouldEqual, Starting)
			validateChanClosed(c, sm.channels.Stopped.channel, false)
			validateChanClosed(c, sm.channels.Starting.channel, true)
		})
	})
}

func validateTransition(sm *StateMachine, newState State, chNewState, chOldState *StateChan) {
	Convey(fmt.Sprintf("When the %s state is set", newState), func(c C) {
		waiting := true
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			chNewState.Wait()
			waiting = false
		}()

		time.Sleep(5 * time.Millisecond)
		So(waiting, ShouldBeTrue)
		sm.Set(newState)

		Convey(fmt.Sprintf("Then the %s channel is closed by the 'Set' call, and it remains closed", newState), func() {
			wg.Wait()
			So(waiting, ShouldBeFalse)
			validateChanClosed(c, chNewState.channel, true)
		})

		Convey("And the previous state channel is created again", func() {
			validateChanClosed(c, chOldState.channel, false)
		})
	})
}
