package kigo

import (
	"encoding/json"
  "net/http"
  "time"

	"github.com/braintree/manners"
	"github.com/julienschmidt/httprouter"
)

const DefaultWebfaceAddress = "0.0.0.0:32601"
const apiPrefix = "/api"

func (c Connection) RunWebface(addr string, terminator chan struct{}) error {
	if addr == "" {
		addr = DefaultWebfaceAddress
	}

	router := httprouter.New()
	c.defineApiRoutes(router)

	server := manners.NewWithServer(&http.Server{
		Addr:           addr,
		Handler:        router,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	})

	if terminator != nil {
		go func() {
			<-terminator
			server.Close()
		}()
	}

	return server.ListenAndServe()
}

func (c Connection) defineApiRoutes(router *httprouter.Router) {
	ping := func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		if err := c.db.DB().Ping(); err != nil {
			response, _ := json.Marshal(map[string]string{"error": err.Error()})
			http.Error(w, string(response), http.StatusServiceUnavailable)
		} else {
			w.Write([]byte("{}\n"))
		}
	}

	router.GET(apiPrefix+"/ping", ping)
}
