package main

import (
	"flag"
	"fmt"
	"goha/hustdb/binlog"
	"goha/hustdb/comm"
	hc "goha/hustdb/healthcheck"
	"goha/internal/httpman"
	"goha/internal/utils"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/cihub/seelog"

	server "goha/server"
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

	cfpath := filepath.Join(conf, "server.json")

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

	srv, err := server.NewServer(fmt.Sprintf(":%d", gconf.Server.Port), gconf.Concurrency)
	if err != nil {
		panic(err)
	}
	srv.Run()
}
