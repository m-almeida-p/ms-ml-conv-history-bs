package projection

import (
	"context"
	"fmt"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
)

// Rebuild recreates conversation_messages from the inbox with the current
// mapping. It is all or nothing: a failure or a cancelled context leaves the
// messages as they were.
func (s *Service) Rebuild(ctx context.Context) (domain.RebuildResult, error) {
	result, err := s.inbox.Rebuild(ctx, s.Project)
	if err != nil {
		return domain.RebuildResult{}, fmt.Errorf("rebuild messages from the inbox: %w", err)
	}

	return result, nil
}
