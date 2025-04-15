package main

import (
	"github.com/opengovern/og-task-template/cloudql/template"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{PluginFunc: template.Plugin})
}
