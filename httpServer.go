package main

import (
	"net/http"
	"log"
)

type WaitForYou struct{}

func (this *WaitForYou) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("error:%s", err)
		}
	}()

	ac := req.URL.Path
	if ac == "/createQueue" {
		CreateQueue(res, req)
		return
	} else if ac == "/updateQueue" {
		UpdateQueue(res, req)
		return
	} else if ac == "/push" {
		Push(res, req)
		return
	} else if ac == "/pop" {
		Pop(res, req)
		return
	} else if ac == "/setVisibilityTime" {
		SetVisibilityTime(res, req)
		return
	} else if ac == "/delMessage" {
		DelMessage(res, req)
		return
	} else if ac == "/delQueue" {
		DelQueue(res, req)
		return
	} else if ac == "/ping" {
		res.Write([]byte("pong"))
		return
	}

	http.NotFound(res, req)
}

