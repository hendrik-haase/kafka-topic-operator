package topic_manager

import (
	"testing"
)

func TestNew(t *testing.T) {
	tm := New()
	defer tm.Close()

	//err := tm.DeleteTopic("testt")

	err := tm.UpsertTopic("test", 2, 1, nil)

	if err != nil {
		t.Error(err)
	}
}
