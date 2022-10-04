package kafka

import (
	"context"
	"testing"

	"github.com/ONSdigital/dp-kafka/v3/mock"
	. "github.com/smartystreets/goconvey/convey"
)

func newMessage(b []byte, offset int64, headerValue string) *mock.MessageMock {
	return &mock.MessageMock{
		ContextFunc:   func() context.Context { return context.Background() },
		GetDataFunc:   func() []byte { return b },
		GetHeaderFunc: func(key string) string { return headerValue },
		OffsetFunc:    func() int64 { return offset },
		MarkFunc:      func() {},
		CommitFunc:    func() {},
		ReleaseFunc:   func() {},
	}
}

func TestIsEmpty(t *testing.T) {
	Convey("IsEmpty returns true for an empty batch", t, func() {
		batchSize := 1
		batch := NewBatch(batchSize)
		So(batch.IsEmpty(), ShouldBeTrue)
	})

	Convey("IsEmpty returns false for a non-empty batch", t, func() {
		batchSize := 1
		batch := NewBatch(batchSize)
		batch.messages = append(batch.messages, newMessage([]byte{1, 2, 3, 4}, 0, ""))
		So(batch.IsEmpty(), ShouldBeFalse)
	})
}

func TestAdd(t *testing.T) {
	Convey("Given an empty batch", t, func() {
		batchSize := 1
		batch := NewBatch(batchSize)

		Convey("When add is called with a valid message", func() {
			batch.Add(newMessage([]byte{1, 2, 3, 4}, 0, ""))

			Convey("The batch contains the expected message.", func() {
				So(batch.Size(), ShouldEqual, 1)
			})
		})
	})
}

func TestCommit(t *testing.T) {
	Convey("Given a batch with two valid messages", t, func() {
		message1 := newMessage([]byte{0, 1, 2, 3}, 1, "")
		message2 := newMessage([]byte{4, 5, 6, 7}, 2, "")

		batchSize := 2
		batch := NewBatch(batchSize)

		batch.Add(message1)
		batch.Add(message2)

		Convey("When commit is called", func() {
			batch.Commit()

			Convey("Then all messages that were present in batch are marked, and last one is committed, which will commit all marks (including the last one)", func() {
				So(message1.MarkCalls(), ShouldHaveLength, 1)
				So(message1.CommitCalls(), ShouldHaveLength, 0)
				So(message2.MarkCalls(), ShouldHaveLength, 0)
				So(message2.CommitCalls(), ShouldHaveLength, 1)
			})
		})
	})
}

func TestClear(t *testing.T) {
	Convey("Given a batch with two valid messages", t, func() {
		message1 := newMessage([]byte{0, 1, 2, 3}, 1, "")
		message2 := newMessage([]byte{4, 5, 6, 7}, 2, "")
		message3 := newMessage([]byte{8, 9, 10, 11}, 3, "")

		batchSize := 2
		batch := NewBatch(batchSize)

		batch.Add(message1)
		batch.Add(message2)

		Convey("When Clear is called", func() {
			batch.Clear()

			Convey("The batch is emptied.", func() {
				So(batch.IsEmpty(), ShouldBeTrue)
				So(batch.IsFull(), ShouldBeFalse)
				So(batch.Size(), ShouldEqual, 0)
			})

			Convey("The batch can be reused", func() {
				batch.Add(message3)

				So(batch.IsEmpty(), ShouldBeFalse)
				So(batch.IsFull(), ShouldBeFalse)
				So(batch.Size(), ShouldEqual, 1)
			})
		})
	})
}

func TestSize(t *testing.T) {
	Convey("Given a batch", t, func() {
		message := newMessage([]byte{1, 2, 3, 4}, 0, "")

		batchSize := 1
		batch := NewBatch(batchSize)

		So(batch.Size(), ShouldEqual, 0)

		Convey("When add is called with a valid message", func() {
			batch.Add(message)

			Convey("The batch size should increase.", func() {
				So(batch.Size(), ShouldEqual, 1)
				batch.Add(message)
				So(batch.Size(), ShouldEqual, 2)
			})
		})
	})
}

func TestIsFull(t *testing.T) {
	Convey("Given a batch with a size of 2", t, func() {
		message := newMessage([]byte{1, 2, 3, 4}, 0, "")

		batchSize := 2
		batch := NewBatch(batchSize)

		So(batch.IsFull(), ShouldBeFalse)

		Convey("When the number of messages added equals the batch size", func() {

			batch.Add(message)
			So(batch.IsFull(), ShouldBeFalse)
			batch.Add(message)

			Convey("The batch should be full.", func() {
				So(batch.IsFull(), ShouldBeTrue)
			})
		})
	})
}
