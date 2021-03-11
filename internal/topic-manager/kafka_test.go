package topic_manager

import "testing"

func TestNew(t *testing.T) {
	tm := New()
	defer tm.Close()

	err := tm.UpsertTopic("testt", 1, 1, nil)
	if err != nil {
		t.Error(err)
	}
}
