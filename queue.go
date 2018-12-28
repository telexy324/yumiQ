package main

import (
	"net/http"
	"github.com/gomodule/redigo/redis"
	"fmt"
	"time"
	"encoding/json"
	"log"
)

var (
	YumiQ   *Yumi //全局调度器
	ReadyQ  *ReadyQueue //全局准备队列管理器
	DelayQ  *DelayQueue //全局延迟队列管理器
	Queue   *Queues //全局队列管理器
)

const (
	OptQueueNames = "SysInfo_queue_names"
)

//每个队列具体配置
type OptionQueue struct {
	QueueName              string
	VisibilityTimeout      string
	MessageRetentionPeriod string
	DelaySeconds           string
}

//队列管理器配置
type Queues struct {
	Option         map[string]OptionQueue
	UpdateQueue    chan map[string]string
	QueueNameCache map[string]string
}

//创建新的队列管理器
func NewQueues() *Queues {
	return &Queues{make(map[string]OptionQueue), make(chan map[string]string), make(map[string]string)}
}

func (this *Queues) init() (err error) {

	//动态更新队列配置
	go func() {
		for {
			select {
			case uo := <-this.UpdateQueue: //监控updateQueue
				this.AddQueueInOpt(uo["queueName"]) //记录新添加的queue，添加到set，记录队列的名字
				this.SaveOptCache(uo["queueName"], uo) //记录到全局队列管理器的配置map中
				go DelayQ.Monitor(uo["queueName"])  //为每个队列创建监视器，每秒查一次，延迟队列时间到后，发布到准备队列
			}
		}
	}()

	items, _ := this.GetAllQueuesInfo()

	if len(items) == 0 {
		return
	}

	var optMap map[string]string
	for _, qname := range items {
		if optMap, err = this.GetOptions(qname); err != nil { //获取所有队列的配置信息
			return
		}
		//更新到内存
		this.SaveOptCache(qname, optMap)
	}
	return
}

func (this *Queues) SaveOptCache(qname string, opt map[string]string) {
	this.Option[qname] = OptionQueue{opt["queueName"], opt["visibilityTimeout"], opt["messageRetentionPeriod"], opt["delaySeconds"]}
}

func (this *Queues) Get(queueName string) (qn OptionQueue, ok bool) {
	qn, ok = this.Option[queueName]
	return
}

//系统配置
func (this *Queues) AddQueueInOpt(qname string) (err error) {
	rdg := Pool.Get()
	defer rdg.Close()
	//记录所有的队列名
	_, err = rdg.Do("SADD", OptQueueNames, qname)

	this.QueueNameCache[qname] = ""

	return
}

func (this *Queues) DelQueueInOpt(k string) (err error) {
	rdg := Pool.Get()
	defer rdg.Close()

	_, err = rdg.Do("SREM", OptQueueNames, k)

	if _, ok := this.QueueNameCache[k]; ok {
		delete(this.QueueNameCache, k)
	}
	return
}

func (this *Queues) ExistsQueueInOpt(qname string) (ok bool, err error) {
	rdg := Pool.Get()
	defer rdg.Close()

	if _, ok = this.QueueNameCache[qname]; !ok {
		ok, err = redis.Bool(rdg.Do("SISMEMBER", OptQueueNames, qname))
	}

	return
}

//获取系统配置，获取队列名set中的队列名，存到全局队列管理器中的队列缓存里（key是队列名，value为空）
func (this *Queues) GetAllQueuesInfo() ([]string, error) {
	rdg := Pool.Get()
	defer rdg.Close()

	queues, err := redis.Strings(rdg.Do("SMEMBERS", OptQueueNames))

	for _, q := range queues {
		this.QueueNameCache[q] = "" //cache
	}
	return queues, err
}

//直接获取全局队列管理器中的队列缓存
func (this *Queues) GetAllQueuesInfoByCache() (map[string]string, error) {

	if len(this.QueueNameCache) == 0 {
		return nil,fmt.Errorf("queues info cache not exists")
	}
	return this.QueueNameCache,nil
}

//删除配置hash中的队列
func (this *Queues) DelQueue(queueName string) (err error) {
	rdg := Pool.Get()
	defer rdg.Close()

	if _, err = rdg.Do("DEL", this.Table(queueName)); err == nil {
		err = this.DelQueueInOpt(queueName)
	}

	return
}

//queues读取配置时规定的配置键名
func (this *Queues) Table(queueName string) string {
	return "configureQueue_" + queueName
}

//获取某队列的配置hash中
func (this *Queues) GetOptions(queueName string) (opt map[string]string, err error) {
	rdg := Pool.Get()
	defer rdg.Close()

	queueName = this.Table(queueName)

	if opt, err = redis.StringMap(rdg.Do("HGETALL", queueName)); err != nil {
		return
	}

	return
}

//查看配置中有无此队列
func (this *Queues) queueExists(queueName string) (bool, error) {
	rdg := Pool.Get()
	defer rdg.Close()

	if result, _ := redis.Bool(rdg.Do("EXISTS", this.Table(queueName))); result {
		return true, fmt.Errorf("queue exists:%s", queueName)
	}

	return false, nil
}

//创建配置
func (this *Queues) build(opt OptionQueue) (err error) {
	rdg := Pool.Get()
	defer rdg.Close()

	queueName := this.Table(opt.QueueName)

	visibilityTimeout, messageRetentionPeriod, delaySeconds := toInt64(opt.VisibilityTimeout), toInt64(opt.MessageRetentionPeriod), toInt64(opt.DelaySeconds)

	if visibilityTimeout == 0 {
		return fmt.Errorf("VisibilityTimeout must be greater than zero!")
	}

	//隐藏时间
	if _, err = rdg.Do("HSET", queueName, "visibilityTimeout", visibilityTimeout); err != nil {
		return
	}
	//信息最大保存时间
	if _, err = rdg.Do("HSET", queueName, "messageRetentionPeriod", messageRetentionPeriod); err != nil {
		return
	}
	//延迟队列
	if _, err = rdg.Do("HSET", queueName, "delaySeconds", delaySeconds); err != nil {
		return
	}
	return
}

func (this *Queues) Create(opt OptionQueue) (err error) {
	if result, _ := this.queueExists(opt.QueueName); result {
		return fmt.Errorf("Queue %s exist", opt.QueueName)
	}
	return this.build(opt)
}

func (this *Queues) Update(opt OptionQueue) (err error) {
	if result, _ := this.queueExists(opt.QueueName); !result {
		return fmt.Errorf("Queue %s doesn't exist", opt.QueueName)
	}
	return this.build(opt)
}

type Yumi struct {
}

func NewYumi() *Yumi {
	return &Yumi{}
}

//定时清理
func (this *Yumi) FunWork() {
	go func(){
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-ticker.C:
				queues, _ := Queue.GetAllQueuesInfoByCache()
				for qname,_ := range queues {
					this.CleanQueue(qname)
				}
			}
		}
	}()
}

//创建队列，调用queues的create方法，先更新redis中的hash
//然后更新queues的configmap,同时更新redis的表名set，并启动一个delayqueue的监视Go程
func (this *Yumi) Create(optionQueue OptionQueue) (err error) {

	if err = Queue.Create(optionQueue); err == nil {
		opt := make(map[string]string)
		opt["queueName"] = optionQueue.QueueName
		opt["visibilityTimeout"] = optionQueue.VisibilityTimeout
		opt["messageRetentionPeriod"] = optionQueue.MessageRetentionPeriod
		opt["delaySeconds"] = optionQueue.DelaySeconds

		Queue.UpdateQueue <- opt //创建
	}
	return
}

func (this *Yumi) Update(optionQueue OptionQueue) (err error) {

	if err = Queue.Update(optionQueue); err == nil {
		opt := make(map[string]string)
		opt["queueName"] = optionQueue.QueueName
		opt["visibilityTimeout"] = optionQueue.VisibilityTimeout
		opt["messageRetentionPeriod"] = optionQueue.MessageRetentionPeriod
		opt["delaySeconds"] = optionQueue.DelaySeconds

		Queue.UpdateQueue <- opt //更新
	}
	return
}

//插入队列
func (this *Yumi) Push(queueName string, body string, delaySeconds string) (err error) {
	optionQueue, ok := Queue.Get(queueName) //获取队列管理器queues中的队列配置

	if !ok {
		return fmt.Errorf("Queue %s exception", queueName)
	}

	var delaySecondsInt, queueDelaySeconds int64

	if delaySeconds != "" {
		delaySecondsInt = toInt64(delaySeconds)
	}

	if optionQueue.DelaySeconds != "" {
		queueDelaySeconds = toInt64(optionQueue.DelaySeconds)
	}

	if delaySecondsInt != 0 {  //入列有延时，按入列延时
		err = this.delayPush(queueName, body, delaySecondsInt)
	} else if queueDelaySeconds != 0 {  //入列没有延时，按队列延时
		err = this.delayPush(queueName, body, queueDelaySeconds)
	} else {  //都没有延时，插入准备队列
		err = this.readyPush(queueName, body)
	}
	return
}

//插入延迟队列
func (this *Yumi) delayPush(queueName string, body string, delaySeconds int64) (err error) {
	if err = DelayQ.Add(queueName, body, delaySeconds); err != nil {
		return
	}
	return
}

//插入准备队列
func (this *Yumi) readyPush(queueName string, body string) (err error) {
	if err = ReadyQ.Push(queueName, body); err != nil {
		return
	}
	return
}

//弹出队列
func (this *Yumi) Pop(queueName string, waitSeconds int) (string, error) {
	body,err := ReadyQ.Pop(queueName, waitSeconds)
	if err != nil{
		return "",err
	}
	//出列后入延迟列
	optionQueue, ok := Queue.Get(queueName)
	if ok {
		DelayQ.Add(queueName, body, toInt64(optionQueue.VisibilityTimeout));
	}
	return body,err
}

//删除
func (this *Yumi) Del(queueName string, body string) (err error) {
	return DelayQ.Del(queueName, body)

}

//
func (this *Yumi) SetVisibilityTime(queueName string, body string, visibilityTime int64) (err error) {
	err = DelayQ.SetVisibilityTime(queueName, body, visibilityTime)
	return
}

func (this *Yumi) CleanQueue(queueName string) (err error) {
	rdg := Pool.Get()
	defer rdg.Close()

	optionQueue,ok := Queue.Get(queueName)
	delayQueueName := DelayQ.Table(queueName)

	if !ok {
		return fmt.Errorf("Queue does not exist")
	}

	holdSecond := toInt64(optionQueue.MessageRetentionPeriod)

	validBySecond := theMoment() - holdSecond

	if holdSecond != 0{
		_,err = rdg.Do("ZREMRANGEBYSCORE", delayQueueName, 0, validBySecond)
	}
	return
}

//删除队列
func (this *Yumi) DelQueue(queueName string) (err error) {
	if err = ReadyQ.DelQueue(queueName); err != nil {
		return
	}

	if err = DelayQ.DelQueue(queueName); err != nil {
		return
	}
	if err = Queue.DelQueue(queueName); err != nil {
		return
	}
	return
}

func (this *Yumi) Write(res http.ResponseWriter, message interface{}) {
	result, err := json.Marshal(message)
	must(err)
	res.Write(result)
}

//准备队列
type ReadyQueue struct {
}

func NewReadyQueue() *ReadyQueue {
	return &ReadyQueue{}
}

func (this *ReadyQueue) Table(queueName string) string {
	return "readyQueue_" + queueName
}

//添加到准备队列
func (this *ReadyQueue) Push(queueName string, id string) (err error) {
	rdg := Pool.Get()
	defer rdg.Close()

	_, err = rdg.Do("LPUSH", this.Table(queueName), id)
	return
}

//出队列
func (this *ReadyQueue) Pop(queueName string, waitSeconds int) (string, error) {
	rdg := Pool.Get()
	defer rdg.Close()

	qn := this.Table(queueName)
	//假如在指定时间内没有任何元素被弹出，则返回一个 nil 和等待时长。 反之，返回一个含有两个元素的列表，第一个元素是被弹出元素所属的 key ，第二个元素是被弹出元素的值。
	values, err := redis.Values(rdg.Do("BRPOP", qn, waitSeconds)) //表示阻塞等待waitsecond秒

	var nul, body string
	redis.Scan(values, &nul, &body)

	return body, err
}

func (this *ReadyQueue) DelQueue(queueName string) (err error) {
	rdg := Pool.Get()
	defer rdg.Close()

	_, err = rdg.Do("DEL", this.Table(queueName))
	return err
}

func (this *ReadyQueue) MultiPush(queueName string, items []string) {
	rdg := Pool.Get()
	defer rdg.Close()

	queueName = this.Table(queueName)
	for _, v := range items {
		rdg.Send("LPUSH", queueName, v)
	}
	rdg.Flush()
}

type DelayQueue struct {
}

func NewDelayQueue() *DelayQueue {
	return &DelayQueue{}
}

func (this *DelayQueue) Table(queueName string) string {
	return "delayQueue_" + queueName
}

//添加
func (this *DelayQueue) Add(queueName string, id string, delaySeconds int64) (err error) {
	redis := Pool.Get()
	defer redis.Close()

	delaySeconds = theMoment() + delaySeconds
	_, err = redis.Do("ZADD", this.Table(queueName), delaySeconds, id)
	return
}

//删除
func (this *DelayQueue) Del(queueName string, id string) (err error) {
	redis := Pool.Get()
	defer redis.Close()

	_, err = redis.Do("ZREM", this.Table(queueName), id)
	return
}

func (this *DelayQueue) SetVisibilityTime(queueName string, body string, visibilityTime int64) (err error) {
	redis := Pool.Get()
	defer redis.Close()

	optionQueue, _ := Queue.Get(queueName)

	if visibilityTime == 0 {
		visibilityTime = toInt64(optionQueue.VisibilityTimeout)
	}

	visibilityTime = theMoment() + visibilityTime
	_, err = redis.Do("ZADD", this.Table(queueName), visibilityTime, body)

	return
}

func (this *DelayQueue) DelQueue(queueName string) (err error) {
	rdg := Pool.Get()
	defer rdg.Close()

	_, err = rdg.Do("DEL", this.Table(queueName))
	return err
}

//移去 准备队列
func (this *DelayQueue) ToReadyQueue(queueName string, exit chan bool) {
	rdg := Pool.Get()
	defer rdg.Close()

	if ok, _ := Queue.ExistsQueueInOpt(queueName); !ok {  //查看队列名set中有无此队列
		close(exit)
		log.Printf("%s queue exit", queueName)
	}

	nowBySecond := theMoment()
	delayQueueName := this.Table(queueName)

	items, err := redis.Strings(rdg.Do("ZRANGEBYSCORE", delayQueueName, 0, nowBySecond))
	must(err)

	if len(items) != 0 {  //把所有的到期队列移到准备队列中
		_, err = redis.Int(rdg.Do("ZREMRANGEBYSCORE", delayQueueName, 0, nowBySecond))
		must(err)
		ReadyQ.MultiPush(queueName, items)
	}
}

//监控队列 每秒监控是否有队列到期
func (this *DelayQueue) Monitor(queueName string) {
	redis := Pool.Get()
	ticker := time.NewTicker(1 * time.Second)

	defer func() {
		redis.Close()
		ticker.Stop()
		Qlock.Unlock()
	}()

	exit := make(chan bool)

	for {
		select {
		case <-ticker.C:
			if err := Qlock.Lock(); err == nil {
				this.ToReadyQueue(queueName, exit)
			}
		case <-exit:
			return
		}
	}
}

//延迟队列定时
func (this *DelayQueue) Trigger() {
	rdg := Pool.Get()
	defer rdg.Close()

	items, _ := Queue.GetAllQueuesInfoByCache()
	for qname,_ := range items {
		go this.Monitor(qname)
	}
}

type CreateResult struct {
	Success                bool   `json:"success"`
	QueueName              string `json:"queueName"`
	VisibilityTimeout      string `json:"visibilityTimeout"`
	MessageRetentionPeriod string `json:"messageRetentionPeriod"`
	DelaySeconds           string `json:"delaySeconds"`
	Error                  string `json:"error"`
}

type UpdateResult struct {
	Success                bool   `json:"success"`
	QueueName              string `json:"queueName"`
	VisibilityTimeout      string `json:"visibilityTimeout"`
	MessageRetentionPeriod string `json:"messageRetentionPeriod"`
	DelaySeconds           string `json:"delaySeconds"`
	Error                  string `json:"error"`
}

type PushResult struct {
	Success      bool   `json:"success"`
	QueueName    string `json:"queueName"`
	Body         string `json:"body"`
	DelaySeconds string `json:"delaySeconds"`
	Error        string `json:"error"`
}

type PopResult struct {
	Success bool   `json:"success"`
	Body    string `json:"body"`
	Error   string `json:"error"`
}

type DelResult struct {
	Success bool   `json:"success"`
	Body    string `json:"body"`
	Error   string `json:"error"`
}

type DelQueueResult struct {
	Success   bool   `json:"success"`
	QueueName string `json:"queueName"`
	Error     string `json:"error"`
}

type SetVisibilityTimeResult struct {
	Success           bool   `json:"success"`
	QueueName         string `json:"queueName"`
	VisibilityTimeout string `json:"visibilityTimeout"`
	Error             string `json:"error"`
}

func CreateQueue(res http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	queueName := req.PostFormValue("QueueName")
	visibilityTimeout := req.PostFormValue("VisibilityTimeout")
	messageRetentionPeriod := req.PostFormValue("MessageRetentionPeriod")
	delaySeconds := req.PostFormValue("DelaySeconds")

	if queueName == "" {
		YumiQ.Write(res, CreateResult{false, queueName, visibilityTimeout, messageRetentionPeriod, delaySeconds, "QueueName must not be null"})
		return
	}

	var optionQueue OptionQueue
	optionQueue.QueueName = queueName
	optionQueue.VisibilityTimeout = visibilityTimeout
	optionQueue.MessageRetentionPeriod = messageRetentionPeriod //最大存储时间
	optionQueue.DelaySeconds = delaySeconds

	if err := YumiQ.Create(optionQueue); err != nil {
		YumiQ.Write(res, CreateResult{false, queueName, visibilityTimeout, messageRetentionPeriod, delaySeconds, err.Error()})
	} else {
		YumiQ.Write(res, CreateResult{true, queueName, visibilityTimeout, messageRetentionPeriod, delaySeconds, ""})
	}
}

func UpdateQueue(res http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	queueName := req.PostFormValue("QueueName")
	visibilityTimeout := req.PostFormValue("VisibilityTimeout")           //变成活跃时间
	messageRetentionPeriod := req.PostFormValue("MessageRetentionPeriod") //信息最大保存时间
	delaySeconds := req.PostFormValue("DelaySeconds")                     //延迟时间

	if queueName == "" {
		YumiQ.Write(res, UpdateResult{false, queueName, visibilityTimeout, messageRetentionPeriod, delaySeconds, "QueueName must not be null"})
		return
	}

	var optionQueue OptionQueue
	optionQueue.QueueName = queueName
	optionQueue.VisibilityTimeout = visibilityTimeout
	optionQueue.MessageRetentionPeriod = messageRetentionPeriod
	optionQueue.DelaySeconds = delaySeconds

	if err := YumiQ.Update(optionQueue); err != nil {
		YumiQ.Write(res, UpdateResult{false, queueName, visibilityTimeout, messageRetentionPeriod, delaySeconds, err.Error()})
	} else {
		YumiQ.Write(res, UpdateResult{true, queueName, visibilityTimeout, messageRetentionPeriod, delaySeconds, ""})
	}
}

func Push(res http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	queueName := req.PostFormValue("queueName")
	body := req.PostFormValue("body")
	delaySeconds := req.PostFormValue("delaySeconds") //延迟时间

	if queueName == "" || body == "" {
		YumiQ.Write(res, PushResult{false, queueName, body, delaySeconds, "queueName and body must not be null"})
		return
	}

	if err := YumiQ.Push(queueName, body, delaySeconds); err != nil {
		YumiQ.Write(res, PushResult{false, queueName, body, delaySeconds, err.Error()})
	} else {
		YumiQ.Write(res, PushResult{true, queueName, body, delaySeconds, ""})
	}
}

func Pop(res http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	queueName := req.Form["queueName"]
	waitSeconds := req.Form["waitSeconds"]

	if len(waitSeconds) == 0 || len(queueName) == 0 {
		YumiQ.Write(res, PopResult{false, "", "queueName or waitSeconds lose"})
		return
	}

	second := toInt64(waitSeconds[0])

	body, err := YumiQ.Pop(queueName[0], int(second))

	if err != nil {
		YumiQ.Write(res, PopResult{false, "", "no news"})
	} else {
		YumiQ.Write(res, PopResult{true, body, ""})
	}
}

func DelMessage(res http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	queueName := req.PostFormValue("queueName")
	body := req.PostFormValue("body")

	if queueName == "" || body == "" {
		YumiQ.Write(res, DelResult{false, "", "queueName and body must not be null"})
		return
	}

	err := YumiQ.Del(queueName, body)
	if err != nil {
		YumiQ.Write(res, DelResult{false, "", err.Error()})

	} else {
		YumiQ.Write(res, DelResult{true, body, ""})
	}
}

func SetVisibilityTime(res http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	queueName := req.PostFormValue("queueName")
	body := req.PostFormValue("body")
	visibilityTime := req.PostFormValue("visibilityTime")

	if queueName == "" || body == "" {
		YumiQ.Write(res, SetVisibilityTimeResult{false, queueName, visibilityTime, "queueName and body must not be null"})
		return
	}

	visibilitySecond := toInt64(visibilityTime)
	err := YumiQ.SetVisibilityTime(queueName, body, visibilitySecond)
	if err != nil {
		YumiQ.Write(res, SetVisibilityTimeResult{false, queueName, visibilityTime, err.Error()})
	} else {
		YumiQ.Write(res, SetVisibilityTimeResult{true, queueName, visibilityTime, ""})
	}
}

func DelQueue(res http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	queueName := req.PostFormValue("queueName")

	if queueName == "" {
		YumiQ.Write(res, DelQueueResult{false, queueName, "queueName and body must not be null"})
		return
	}

	err := YumiQ.DelQueue(queueName)
	if err != nil {
		YumiQ.Write(res, DelQueueResult{false, queueName, err.Error()})
	} else {
		YumiQ.Write(res, DelQueueResult{true, queueName, ""})
	}
}
