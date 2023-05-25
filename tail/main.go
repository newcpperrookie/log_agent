package tail

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
	"log_agent/common"
	"log_agent/kafka"
	"strings"
	"time"
)

// 日志收集项
type tailTask struct {
	path    string
	topic   string
	TailObj *tail.Tail
	ctx     context.Context
	cancel  context.CancelFunc
}

var (
	confChan chan []common.CollectEntry
)

func newTailTask(conf common.CollectEntry, tailCfg tail.Config) (tt *tailTask, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	tt = &tailTask{
		path:   conf.Path,
		topic:  conf.Topic,
		ctx:    ctx,
		cancel: cancel,
	}
	tt.TailObj, err = tail.TailFile(conf.Path, tailCfg)
	if err != nil {
		logrus.Errorf("tailfile: init %s failed, err: %v\n", conf.Path, err)
		return nil, err
	}
	return tt, nil
}

func (tt *tailTask) run() {
	//读取日志发往kafka
	for {
		select {
		case <-tt.ctx.Done():
			logrus.Infof("the collect task : %s is terminated", tt.path)
			return
		case line, ok := <-tt.TailObj.Lines:
			if !ok {
				logrus.Errorf("tail failed closed reopen , filename : %s\n", tt.TailObj.Filename)
				time.Sleep(time.Second)
				continue
			}
			//去掉空行
			if len(strings.Trim(line.Text, "\r")) == 0 {
				continue
			}
			logrus.Infof("the tt: %v, topic: %v, send msg: %v", tt.path, tt.topic, line.Text)
			msg := &sarama.ProducerMessage{}
			msg.Topic = tt.topic
			msg.Value = sarama.StringEncoder(line.Text)
			//利用通道
			kafka.MsgChan(msg)
		}
	}
}
func NewConf(newConf []common.CollectEntry) {
	ttmgr.confChan <- newConf
}
