package kafkatest

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMessageMock(t *testing.T) {
	Convey("Given a message mock", t, func() {
		payload := []byte{0, 1, 2, 3, 4, 5}
		msg, err := NewMessage(payload, 1)
		if err != nil {
			t.Errorf("%s", errNewMessage)
		}

		Convey("The initial state is not marked or committed", func() {
			So(msg.marked, ShouldBeFalse)
			So(msg.committed, ShouldBeFalse)
		})

		Convey("GetData returns the payload", func() {
			So(msg.GetData(), ShouldResemble, payload)
		})

		Convey("Offset returns the message offset", func() {
			So(msg.Offset(), ShouldEqual, 1)
		})

		Convey("UpstreamDone returns the upstreamDone channel", func() {
			So(msg.UpstreamDone(), ShouldResemble, msg.upstreamDoneChan)
		})

		Convey("Mark marks the message as consumed, but doesn't commit it", func() {
			msg.Mark()
			So(msg.marked, ShouldBeTrue)
			So(msg.committed, ShouldBeFalse)
		})

		Convey("Commit marks the message as consumed and commits it", func() {
			msg.Commit()
			So(msg.marked, ShouldBeTrue)
			So(msg.committed, ShouldBeTrue)
		})

		Convey("Release closes the upstreamChannel but doesn't mark the message as consumed and doesn't commit it", func() {
			go func() {
				msg.Release()
			}()
			_, ok := <-msg.upstreamDoneChan
			So(ok, ShouldBeFalse)
			So(msg.marked, ShouldBeFalse)
			So(msg.committed, ShouldBeFalse)
		})

		Convey("CommitAndRelease marks the message as consumed, commits it, and closes the upstreamChannel", func() {
			go func() {
				msg.CommitAndRelease()
			}()
			_, ok := <-msg.upstreamDoneChan
			So(ok, ShouldBeFalse)
			So(msg.marked, ShouldBeTrue)
			So(msg.committed, ShouldBeTrue)
		})

		Convey("IsMarked returns true if the message is marked as consumed, false otherwise", func() {
			So(msg.IsMarked(), ShouldBeFalse)
			msg.marked = true
			So(msg.IsMarked(), ShouldBeTrue)
		})

		Convey("IsCommitted returns true if the message is committed, false otherwise", func() {
			So(msg.IsCommitted(), ShouldBeFalse)
			msg.committed = true
			So(msg.IsCommitted(), ShouldBeTrue)
		})
	})
}
