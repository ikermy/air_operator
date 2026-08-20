package http

import (
	metrics2 "air_operator/internal/metrics"
	stdhttp "net/http"

	"github.com/ikermy/air_logger/v2/pkg/logger"
)

// Listener provides the operator handlers needed by the HTTP transport.
type Listener interface {
	StartOperators() error
	Available(stdhttp.ResponseWriter, *stdhttp.Request)
	Events(stdhttp.ResponseWriter, *stdhttp.Request)
	Message(stdhttp.ResponseWriter, *stdhttp.Request)
}

type Server struct {
	listener Listener
}

func New(listener Listener) *Server { return &Server{listener: listener} }

func (s *Server) StartOperators() error { return s.listener.StartOperators() }

func enableCORS(next stdhttp.HandlerFunc) stdhttp.HandlerFunc {
	return func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == stdhttp.MethodOptions {
			w.WriteHeader(stdhttp.StatusNoContent)
			return
		}
		next(w, r)
	}
}

func (s *Server) Listener() {
	logger.Infoln("Web server AiR_Operator started")

	stdhttp.Handle("/metrics", metrics2.Handler())
	stdhttp.Handle("/oper/available", metrics2.HTTPMiddleware("/available", enableCORS(s.listener.Available)))
	stdhttp.Handle("/op", metrics2.HTTPMiddleware("/op", enableCORS(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		switch r.Method {
		case stdhttp.MethodGet:
			s.listener.Events(w, r)
		case stdhttp.MethodPost:
			s.listener.Message(w, r)
		case stdhttp.MethodOptions:
			w.WriteHeader(stdhttp.StatusOK)
		default:
			stdhttp.Error(w, "Метод не разрешен", stdhttp.StatusMethodNotAllowed)
		}
	})))

	if err := stdhttp.ListenAndServe(":8080", nil); err != nil {
		logger.Error("ошибка запуска WEB сервера AiR_Operator: %v", err)
	}
}
