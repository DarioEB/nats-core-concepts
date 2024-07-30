package main

import (
	"encoding/json"
	"fmt"
	"go-scripts-connections-sv/nats_client"
	"log"
	"net/http"
	"time"

	"github.com/nats-io/nats.go"
)

type (
	BodyParams struct {
		Subject string `json:"subject"`
		Message string `json:"message"`
	}
	SubjectArray struct {
		Subjects []BodyParams `json:"subjects"`
	}
	ErrorResponse struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	SuccessResponse struct {
		Data map[string]interface{} `json:"message"`
	}
)

func main() {
	mux := http.NewServeMux()
	nats_conn, err := nats_client.NewConnection(
		"localhost",
		"4222",
		"",
		"",
	)

	if err != nil {
		log.Fatalf("Error connecting to Nats: %v", err)
	}

	if nats_conn == nil {
		log.Fatal("Nats connection is nil")
	}
	fmt.Println("Nats connected to: ", nats_conn.ListServers()[0])

	defer nats_conn.CloseConnection()

	if err != nil {
		log.Fatalf(err.Error())
	}

	mux.HandleFunc("/api/core/publish-message", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(404)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error:   "invalid_method",
				Message: "Invalid method",
			})
			return
		}

		var bodyparams BodyParams
		if err := json.NewDecoder(r.Body).Decode(&bodyparams); err == nil {
			nats_conn.PublishMessage(bodyparams.Subject, bodyparams.Message)
		}

		w.WriteHeader(201)
		json.NewEncoder(w).Encode(SuccessResponse{
			Data: map[string]interface{}{
				"message": "Send message success",
				"text":    bodyparams.Message,
				"subject": bodyparams.Subject,
			},
		})
	})

	mux.HandleFunc("/api/core/reply", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(404)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error:   "invalid_method",
				Message: "Invalid method",
			})
			return
		}

		var bodyparams BodyParams
		if err := json.NewDecoder(r.Body).Decode(&bodyparams); err == nil {
			nats_conn.PublishMessage(bodyparams.Subject, bodyparams.Message)
		}

		sub := nats_conn.CreateSubscribeReply(bodyparams.Subject, bodyparams.Message)

		done := make(chan bool)
		go func() {
			time.Sleep(30 * time.Second)
			sub.UnsubscribeReply()
			done <- true
		}()

		w.WriteHeader(201)
		json.NewEncoder(w).Encode(SuccessResponse{
			Data: map[string]interface{}{
				"message": "subscribe reply created",
				"subject": bodyparams.Subject,
			},
		})

		go func() {
			<-done
		}()
	})

	mux.HandleFunc("/api/core/request-reply", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(404)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error:   "invalid_method",
				Message: "Invalid method",
			})
			return
		}

		var rep *nats.Msg
		fmt.Println(r.Body)
		var bodyparams BodyParams
		if err := json.NewDecoder(r.Body).Decode(&bodyparams); err != nil {
			w.WriteHeader(400)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error:   "invalid_params",
				Message: "Invalid parameters",
			})
			return
		}
		rep, err = nats_conn.CreateRequest(bodyparams.Subject)
		if err != nil {
			w.WriteHeader(503)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error:   err.Error(),
				Message: "Error type no responders",
			})
			return
		}

		w.WriteHeader(201)
		json.NewEncoder(w).Encode(SuccessResponse{
			Data: map[string]interface{}{
				"message": rep,
				"subject": bodyparams.Subject,
				"reply":   string(rep.Data),
			},
		})
	})

	mux.HandleFunc("/api/core/queue-group", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(404)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error:   "invalid_method",
				Message: "Invalid method",
			})
			return
		}
		var subs []*nats_client.SubscribeReply
		var bodyparams SubjectArray
		if err := json.NewDecoder(r.Body).Decode(&bodyparams); err != nil {
			w.WriteHeader(400)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error:   "invalid_params",
				Message: "Invalid parameters",
			})
			return
		}

		fmt.Println(bodyparams.Subjects)

		for i := range bodyparams.Subjects {
			sub := nats_conn.CreateSubscribeReply(
				bodyparams.Subjects[i].Subject,
				bodyparams.Subjects[i].Message,
			)
			subs = append(subs, sub)
			fmt.Println(subs)
		}

		fmt.Println(subs)

		done := make(chan bool)
		go func() {
			time.Sleep(30 * time.Second)
			for i := range subs {
				subs[i].UnsubscribeReply()
			}
			done <- true
		}()

		w.WriteHeader(201)
		json.NewEncoder(w).Encode(SuccessResponse{
			Data: map[string]interface{}{
				"message":  "Queue Group created",
				"elements": subs,
			},
		})

		go func() {
			<-done
		}()
	})

	s := &http.Server{
		Addr:           ":4444",
		Handler:        mux,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	log.Fatal(s.ListenAndServe())
}
