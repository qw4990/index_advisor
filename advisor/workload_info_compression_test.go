package advisor

import (
	"testing"
)

func Test_getTemplate(t *testing.T) {
	query := "SELECT name FROM users WHERE age > 18 AND city = 'New York' AND hash = '1234'"
	expected := "SELECT name FROM users WHERE age > # AND city = &&& AND hash = &&&"
	result := getTemplate(query)
	if result != expected {
		t.Errorf("Template mismatch. Expected: %v, but got: %v", expected, result)
	}
}
