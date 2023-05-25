package tail

import (
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
	"log_agent/common"
)

//tailTask manager

type tailTaskManager struct {
	tTasksMap        map[string]*tailTask
	collectEntryList []common.CollectEntry
	confChan         chan []common.CollectEntry
}

var ttmgr *tailTaskManager

func Init(logCfg []common.CollectEntry) (err error) {
	ttmgr = &tailTaskManager{
		tTasksMap:        make(map[string]*tailTask, 20),
		collectEntryList: logCfg,
		confChan:         make(chan []common.CollectEntry),
	}
	config := tail.Config{
		ReOpen:    true,
		Follow:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
		MustExist: false,
		Poll:      true}
	for _, conf := range logCfg {
		tt, err := newTailTask(conf, config)
		if err != nil {
			logrus.Errorf("failed to create tt %v", tt)
			continue
		}
		logrus.Infof("create a tail task for path: %s success", tt.path)
		ttmgr.tTasksMap[tt.path] = tt
		go tt.run()
	}
	go ttmgr.watch()
	return
}

func (t *tailTaskManager) watch() {
	for {
		newConf := <-t.confChan
		logrus.Infof("get new conf from etcd : %v", newConf)
		config := tail.Config{
			ReOpen:    true,
			Follow:    true,
			Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
			MustExist: false,
			Poll:      true}
		for _, conf := range newConf {
			//1.原本有的现在也有就不动
			if t.isExist(conf) {
				continue
			}
			//2.原来没有的现在有的要添加
			tt, err := newTailTask(conf, config)
			if err != nil {
				logrus.Errorf("failed to create tt %v", tt)
				continue
			}
			logrus.Infof("create a new tail task for path: %s success", tt.path)
			ttmgr.tTasksMap[tt.path] = tt
			go tt.run()
		}
		//3.原来有的现在没有的要删除
		for _, tt := range ttmgr.tTasksMap {
			var isFound bool = false
			for _, nConf := range newConf {
				if tt.path == nConf.Path {
					isFound = true
					break
				}
			}
			if !isFound {
				tt.cancel()
			}
		}
	}

}

func (t *tailTaskManager) isExist(conf common.CollectEntry) bool {
	_, ok := t.tTasksMap[conf.Path]
	return ok
}
