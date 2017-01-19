package main

import (
	"flag"
	"fmt"
	"hustdbha/httpman"
	"hustdbha/hustdb/binlog"
	"hustdbha/hustdb/comm"
	hc "hustdbha/hustdb/healthcheck"
	"hustdbha/utils"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/cihub/seelog"
)

func main() {
	file, _ := exec.LookPath(os.Args[0])
	path, _ := filepath.Abs(file)
	root := filepath.Dir(path)

	var conf string
	flag.StringVar(&conf, "conf", "", "")
	flag.Parse()

	if "" == conf {
		conf = filepath.Join(root, "conf")
	} else {
		conf, _ = filepath.Abs(conf)
	}

	utils.SetGlobalConfPath(conf)

	logger, err := seelog.LoggerFromConfigAsFile(filepath.Join(conf, "log.xml"))

	if err != nil {
		seelog.Critical("err parsing config log file", err)
		return
	}

	seelog.ReplaceLogger(logger)
	defer seelog.Flush()

	cfpath := filepath.Join(conf, "ha.json")

	if !utils.LoadGlobalConf(cfpath) {
		seelog.Critical("LoadGlobalConf error")
		return
	}

	gconf := utils.GetGlobalConf()

	fmt.Printf("global conf :%v\n", gconf)

	comm.HustdbInit(&gconf.Hustdb)
	hc.Init(gconf.HealthCheck.HealthCheckCycle)
	binlog.Init(gconf.Binlog)
	httpman.InitHttp(gconf.Http, gconf.HealthCheck.Timeout)
}
