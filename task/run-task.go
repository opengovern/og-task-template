package task

import (
	"github.com/opengovern/og-util/pkg/jq"
	"github.com/opengovern/og-util/pkg/opengovernance-es-sdk"
	"github.com/opengovern/og-util/pkg/tasks"
	"github.com/opengovern/opensecurity/services/tasks/scheduler"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

func RunTask(ctx context.Context, jq *jq.JobQueue, coreServiceEndpoint string, esClient opengovernance.Client, logger *zap.Logger, request tasks.TaskRequest, response *scheduler.TaskResponse) error {

	return nil
}
