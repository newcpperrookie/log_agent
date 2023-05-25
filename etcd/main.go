package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log_agent/common"
	"log_agent/tail"
	"time"
)

var (
	cli *clientv3.Client
	err error
)

func Init(address []string) error {
	cli, err = clientv3.New(clientv3.Config{
		Endpoints:   address,
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		fmt.Printf("connect to etcd faild, err :  %v\n", err)
		return err
	}
	return nil
}

func GetConf(key string) (collectEntryList []common.CollectEntry, err error) {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	resp, err := cli.Get(ctx, key)
	if err != nil {
		logrus.Errorf("connect to etcd faild key : %s, err :  %v\n", key, err)
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		logrus.Warningf("get len 0 : %s", key)
		return nil, err
	}
	res := resp.Kvs[0]
	err = json.Unmarshal(res.Value, &collectEntryList)
	fmt.Printf("config: %v", res.Value)
	if err != nil {
		logrus.Errorf("json unmarshal failed : %s, err :  %v\n", key, err)
		return nil, err
	}
	return collectEntryList, nil
}

// 监控etcd中的配置变化
func Watching(key string) {
	for {
		watchCh := cli.Watch(context.Background(), key)
		var conf []common.CollectEntry
		for wResp := range watchCh {
			for _, v := range wResp.Events {
				fmt.Printf("type: %s key : %s value : %s \n", v.Type, v.Kv.Key, v.Kv.Value)
				err := json.Unmarshal(v.Kv.Value, &conf)
				if err != nil {
					logrus.Errorf("json unmarshal newconf failed : %s, err :  %v\n", key, err)
					continue
				}
				tail.NewConf(conf)
			}
		}
	}
}
