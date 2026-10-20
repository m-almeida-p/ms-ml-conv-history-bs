package config

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/testsupport/concurrent"
)

// Load reads the environment and the file and returns a fresh value. Loading at
// once, as two binaries or a reload would, must give the same configuration.
func TestLoadGivesTheSameWhenCalledAtOnce(t *testing.T) {
	clearEnv(t)
	t.Setenv("POSTGRES_PASSWORD", "pa #ss")
	t.Setenv("SERVER_PORT", "8080")

	type loaded struct {
		Config Config
		Err    string
	}
	scenario := func(exec concurrent.Executor) []loaded {
		return concurrent.Collect(exec, 32, func(int) loaded {
			cfg, err := Load(repoConfig)
			if err != nil {
				return loaded{Err: err.Error()}
			}

			return loaded{Config: *cfg}
		})
	}

	outcomes := concurrent.Outcomes(scenario, concurrent.All)

	for i, executor := range concurrent.All {
		assert.Equal(t, outcomes[0], outcomes[i], "%s: the same configuration", executor.Name)
	}
	assert.Empty(t, outcomes[0][0].Err)
	assert.Equal(t, "pa #ss", outcomes[0][0].Config.Postgres.Password)
	assert.Equal(t, 8080, outcomes[0][0].Config.Server.Port)
}
