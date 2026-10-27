package domain

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvalidRequestMatchesTheSentinel(t *testing.T) {
	err := InvalidRequest("page[size] must be a whole number from 1 to %d", 100)

	assert.ErrorIs(t, err, ErrInvalidRequest)
	assert.NotErrorIs(t, err, ErrInvalidCursor)
	assert.NotErrorIs(t, err, ErrNotFound)
}

func TestInvalidRequestCarriesItsDetailAndFormatsIt(t *testing.T) {
	err := InvalidRequest("%s is given more than once", "page[size]")

	var invalid *InvalidRequestError
	require.ErrorAs(t, err, &invalid)
	assert.Equal(t, "page[size] is given more than once", invalid.Detail)
	assert.Equal(t, "invalid request: page[size] is given more than once", err.Error(), "the log keeps the sentinel's words in front")
}

func TestInvalidRequestWithoutArgumentsKeepsAPercentSignAsItIs(t *testing.T) {
	var invalid *InvalidRequestError

	require.ErrorAs(t, InvalidRequest("100%% sure"), &invalid)
	assert.Equal(t, "100% sure", invalid.Detail)
}

func TestTheDetailSurvivesContextWrappedAroundIt(t *testing.T) {
	err := fmt.Errorf("list messages: %w", fmt.Errorf("read page: %w", InvalidRequest("both cursors")))

	var invalid *InvalidRequestError
	require.ErrorAs(t, err, &invalid)
	assert.Equal(t, "both cursors", invalid.Detail, "the context in front is not part of the detail")
	assert.ErrorIs(t, err, ErrInvalidRequest)
}

func TestTheBareSentinelHasNoDetail(t *testing.T) {
	for name, err := range map[string]error{
		"bare":              ErrInvalidRequest,
		"wrappedWithWords":  fmt.Errorf("%w: some words", ErrInvalidRequest),
		"wrappedWithPrefix": fmt.Errorf("context: %w", ErrInvalidRequest),
	} {
		var invalid *InvalidRequestError

		assert.ErrorIs(t, err, ErrInvalidRequest, name)
		assert.False(t, errors.As(err, &invalid), "%s carries no detail, so nothing can be shown", name)
	}
}
