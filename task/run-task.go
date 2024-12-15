package task

import (
	"github.com/opengovern/opencomply/services/tasks/scheduler"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

func RunTask(ctx context.Context, logger *zap.Logger, request scheduler.TaskRequest, response *scheduler.TaskResponse) error {
	// TODO: Implement run task

	response.Result = []byte("Implement RunTask")

	return nil
}
