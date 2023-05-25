package main

import (
	"fmt"
	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
	"log_agent/etcd"
	"log_agent/kafka"
	"log_agent/tail"
)

// 日志收集的客户端
// 类似的开源项目有filebeat

type Config struct {
	KafkaConfig `ini:"kafka"`
	//Collect     `ini:"collect"`
	EtcdConfig `ini:"etcd"`
}
type KafkaConfig struct {
	Address  string `ini:"address"`
	ChanSize uint32 `ini:"chan_size"`
}

type EtcdConfig struct {
	Address    string `ini:"address"`
	CollectKey string `ini:"collect_key"`
}

/*type Collect struct {
	LogFilePath string `ini:"logfile_path"`
}*/

func main() {
	//0.读配置文件
	var ConfigObj = new(Config)
	err := ini.MapTo(ConfigObj, "./conf/config.ini")
	if err != nil {
		logrus.Error("load config failed, err : %v", err)
	}
	fmt.Printf("%#v\n", ConfigObj)
	//1.初始化
	err = kafka.Init([]string{ConfigObj.KafkaConfig.Address}, ConfigObj.KafkaConfig.ChanSize)
	if err != nil {
		logrus.Errorf("init kafka failed, err : %v", err)
		return
	}
	logrus.Info("init kafka success")
	//1.1初始化etcd配置连接
	err = etcd.Init([]string{ConfigObj.EtcdConfig.Address})
	if err != nil {
		logrus.Errorf("init etcd failed, err : %v\n", err)
		return
	}
	//1.2从etcd中拉取要收集日志的配置项
	logCfg, err := etcd.GetConf(ConfigObj.EtcdConfig.CollectKey)
	if err != nil {
		logrus.Errorf("get log_conf failed, err : %v", err)
		return
	}
	//监控etcd的配置变化
	go etcd.Watching(ConfigObj.EtcdConfig.CollectKey)
	//fmt.Printf("%#v", logCfg)
	//2.根据配置中的日志路径使用tail去收集日志
	err = tail.Init(logCfg)
	if err != nil {
		logrus.Errorf("init tail failed, err : %v", err)
		return
	}
	logrus.Info("init tail success")
	//3.将日志数据发往kafka
	for {
		select {}
	}

}
