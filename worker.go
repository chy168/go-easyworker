package worker

import (
	"encoding/json"
	"fmt"
	"github.com/crowdmob/goamz/sqs"
	"github.com/garyburd/redigo/redis"
	"log"
	"sync"
	"time"
)

var (
	workers map[string]HandlerFunc
)

func init() {
	workers = make(map[string]HandlerFunc)
}

type Handler interface {
	HandleMessage(msg *sqs.Message) error
}

type HandlerFunc func(msg *sqs.Message) error

func (f HandlerFunc) HandleMessage(msg *sqs.Message) error {
	return f(msg)
}

func Register(class string, handler HandlerFunc) {
	workers[class] = handler
}

func Start(q *sqs.Queue, t time.Duration, receiveMessageNum int) {
	fmt.Println(fmt.Sprintf("worker: Start polling  [%s]", time.Now().Local()))
	// init redis
	pool := redis.Pool{
		MaxIdle:     3,
		MaxActive:   0, // When zero, there is no limit on the number of connections in the pool.
		IdleTimeout: 30 * time.Second,
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", "127.0.0.1:6379")
			if err != nil {
				log.Fatal(err.Error())
			}
			return conn, err
		},
	}
	for {
		resp, err := q.ReceiveMessage(receiveMessageNum)
		if err != nil {
			log.Println(err)
			continue
		}
		if len(resp.Messages) > 0 {

			fmt.Printf("\r")
			run(q, resp, pool)
		} else {
			fmt.Printf(".")
		}
		time.Sleep(t)
	}
}

// poll launches goroutine per received message and wait for all message to be processed
// dispatcher
func run(q *sqs.Queue, resp *sqs.ReceiveMessageResponse, redisPool redis.Pool) {
	var wg sync.WaitGroup
	for i := range resp.Messages {

		wg.Add(1)
		go func(m *sqs.Message) {

			var job BackgroundJob
			err := json.Unmarshal([]byte(m.Body), &job)
			if err != nil {
				fmt.Printf("unmarshal job json error:%v\n", err)
			}

			if h, ok := workers[job.Type]; ok {

				if err := handleMessage(q, m, h); err != nil {
					log.Fatalf(" *** Message ID : %s  handleMessage error : %s", m.MessageId, err.Error())
				}

			} else {
				fmt.Printf("check register job error:%v\n", err)
			}

			wg.Done()
		}(&resp.Messages[i])
	}
	wg.Wait()
}

func handleMessage(q *sqs.Queue, m *sqs.Message, h Handler) error {
	var err error
	err = h.HandleMessage(m)
	if err != nil {
		return err
	}
	// delete
	_, err = q.DeleteMessage(m)
	return err
}

type BackgroundJob struct {
	Type string
	Data interface{}
}
