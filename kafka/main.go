package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

var (
	client  sarama.SyncProducer
	msgChan chan *sarama.ProducerMessage
)

func Init(address []string, chanSize uint32) (err error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          //ack
	config.Producer.Partitioner = sarama.NewRandomPartitioner //分区
	config.Producer.Return.Successes = true                   //确认

	client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		logrus.Error("生产者连接失败,错误是", err)
		return
	}
	msgChan = make(chan *sarama.ProducerMessage, chanSize)
	go send()
	return
	//defer Client.Close()
}
func send() {
	for {
		select {
		case msg := <-msgChan:
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				logrus.Warning("消息发送失败，错误是", err)
				return
			}
			logrus.Printf("send success, pid: %v, offset: %v\n", pid, offset)
		}
	}
}

func MsgChan(msg *sarama.ProducerMessage) {
	msgChan <- msg
}
