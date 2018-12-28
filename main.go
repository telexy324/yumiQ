package main

import (
	"net/http"
	"time"
	"log"
	"flag"
	"github.com/gomodule/redigo/redis"
	"github.com/hjr265/redsync.go/redsync"
)

var (
	Host  string
	Port  string
	Redis string
	Auth  string
)

func init() {

	log.SetFlags(log.LstdFlags)
	flag.StringVar(&Host, "host", "localhost", "Bound IP. default:localhost")
	flag.StringVar(&Port, "port", "9394", "port. default:9394")
	flag.StringVar(&Redis, "redis", "127.0.0.1:6379", "redis server. default:127.0.0.1:6379")
	flag.StringVar(&Auth, "auth", "", "redis server auth password")

	flag.Parse()
	Pool = newPool(Redis) //创建redis连接池
	log.Printf("Success:redis have connected")

	var err error
	redisPool := []*redis.Pool{Pool}
	Qlock, err = redsync.NewMutexWithPool("redsync", redisPool)
	if err != nil{
		log.Fatalf("redsync error: %v", err)
	}

	YumiQ = NewYumi()
	ReadyQ = NewReadyQueue()
	DelayQ = NewDelayQueue()
	Queue = NewQueues()

	if err := Queue.init(); err != nil {
		log.Fatalf("queue init error: %s", err.Error())
	}

	DelayQ.Trigger()
	YumiQ.FunWork() //暂去掉
}

func main() {

	//runtime.GOMAXPROCS(runtime.NumCPU())

	s := &http.Server{
		Addr:           Host + ":" + Port,
		Handler:        &WaitForYou{},
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	log.Printf("Success:HTTP has been started")
	log.Fatal(s.ListenAndServe())

}

/*
分3个存储方式
1.set用于存储队列的名字
2.hash用于存储队列的配置信息
	QueueName              string
	VisibilityTimeout      string
	MessageRetentionPeriod string
	DelaySeconds           string
3.zset用于存储延迟队列
*/