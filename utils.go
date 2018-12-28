package main

import (
	"time"
	"strconv"
	"github.com/gomodule/redigo/redis"
	"github.com/hjr265/redsync.go/redsync"
	"log"
)

var (
	Pool  *redis.Pool
	Qlock *redsync.Mutex
)


func newPool(server string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     8,
		MaxActive:   0,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", server)
			if err != nil {
				log.Printf("Fail:redis connection failed")
				return nil, err
			}

			if Auth != "" {
				if _, err := conn.Do("AUTH", Auth); err != nil {
					conn.Close()
					log.Printf("Fail:redis auth failed")
					return nil, err
				}
			}
			return conn, err
		},
		//池中的连接再次启用前，通过设置此选项检查连接状况
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func must(e error) {
	if e != nil {
		panic(e)
	}
}

func theMoment() int64{
	return time.Now().Unix()
}

func toInt64(parameters string) int64 {
	if parameter, err := strconv.ParseInt(parameters, 10, 32); err != nil {
		return 0
	} else {
		return parameter
	}
}

func toString(parameters int64) string{
	return strconv.FormatInt(parameters,10)
}

