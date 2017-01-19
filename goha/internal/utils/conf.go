package utils

import (
	def "goha/internal/defines"
)

type HaConf struct {
	Hustdb      def.HustdbConf
	Http        def.HttpConf
	HealthCheck def.HealthCheckConf
	Binlog      def.BinlogConf
}

var configPath string

func SetGlobalConfPath(path string) {
	configPath = path
}

func GetGlobalConfPath() string {
	return configPath
}

var globalhaconfig *HaConf

func LoadGlobalConf(path string) bool {
	globalhaconfig = new(HaConf)
	return LoadConf(path, globalhaconfig)
}

func GetGlobalConf() *HaConf {
	return globalhaconfig
}
