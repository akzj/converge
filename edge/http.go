package edge

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	coreerrors "github.com/cockroachdb/errors"

	"github.com/akzj/converge/pkg/model"
)

const maxRequestBody = 16 << 20

type HTTPHandler struct {
	runtime *Runtime
	token   string
	mux     *http.ServeMux
}

// NewHTTPHandler creates an authenticated transport for the Edge Runtime.
// TLS and token rotation are owned by the embedding agent/server.
func NewHTTPHandler(runtime *Runtime, bearerToken string) (*HTTPHandler, error) {
	if runtime == nil {
		return nil, coreerrors.New("edge HTTP runtime is nil")
	}
	if strings.TrimSpace(bearerToken) == "" {
		return nil, coreerrors.New("edge HTTP bearer token is empty")
	}
	h := &HTTPHandler{runtime: runtime, token: bearerToken, mux: http.NewServeMux()}
	h.mux.HandleFunc("POST /v1/desired-snapshots", h.submitSnapshot)
	h.mux.HandleFunc("GET /v1/desired-snapshots/current", h.currentSnapshot)
	h.mux.HandleFunc("GET /v1/status", h.status)
	h.mux.HandleFunc("GET /v1/status/{config}", h.configStatus)
	h.mux.HandleFunc("POST /v1/commands/refresh/{config}", h.refresh)
	h.mux.HandleFunc("GET /healthz", h.health)
	h.mux.HandleFunc("GET /readyz", h.ready)
	h.mux.HandleFunc("GET /metrics", h.metrics)
	return h, nil
}

func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != "/healthz" && !h.authorized(req) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"code": "unauthorized"})
		return
	}
	h.mux.ServeHTTP(w, req)
}

func (h *HTTPHandler) authorized(req *http.Request) bool {
	const prefix = "Bearer "
	header := req.Header.Get("Authorization")
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	provided := strings.TrimPrefix(header, prefix)
	return len(provided) == len(h.token) && subtle.ConstantTimeCompare([]byte(provided), []byte(h.token)) == 1
}

func (h *HTTPHandler) submitSnapshot(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(w, req.Body, maxRequestBody))
	decoder.DisallowUnknownFields()
	var snapshot model.DesiredSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		writeJSON(w, http.StatusBadRequest, SnapshotACK{Code: "invalid_json", Reason: err.Error()})
		return
	}
	if err := ensureJSONEOF(decoder); err != nil {
		writeJSON(w, http.StatusBadRequest, SnapshotACK{Code: "invalid_json", Reason: err.Error()})
		return
	}
	ack := h.runtime.SubmitSnapshot(req.Context(), snapshot)
	status := http.StatusAccepted
	if !ack.Accepted {
		switch ack.Code {
		case "revision_conflict":
			status = http.StatusConflict
		case "invalid_snapshot":
			status = http.StatusBadRequest
		default:
			status = http.StatusServiceUnavailable
		}
	} else if ack.Code == "duplicate" {
		status = http.StatusOK
	}
	writeJSON(w, status, ack)
}

func (h *HTTPHandler) currentSnapshot(w http.ResponseWriter, req *http.Request) {
	identity, err := h.runtime.CurrentSnapshot(req.Context())
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"code": "storage_unavailable", "reason": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, identity)
}

func (h *HTTPHandler) status(w http.ResponseWriter, req *http.Request) {
	status, err := h.runtime.Status(req.Context())
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"code": "storage_unavailable", "reason": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (h *HTTPHandler) configStatus(w http.ResponseWriter, req *http.Request) {
	report, ok := h.runtime.ConfigStatus(req.PathValue("config"))
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"code": "config_not_found"})
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (h *HTTPHandler) refresh(w http.ResponseWriter, req *http.Request) {
	if err := h.runtime.Refresh(req.Context(), req.PathValue("config")); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"code": "config_not_found", "reason": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"code": "accepted"})
}

func (h *HTTPHandler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *HTTPHandler) ready(w http.ResponseWriter, _ *http.Request) {
	if !h.runtime.Ready() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (h *HTTPHandler) metrics(w http.ResponseWriter, req *http.Request) {
	status, err := h.runtime.Status(req.Context())
	if err != nil {
		http.Error(w, "status unavailable", http.StatusServiceUnavailable)
		return
	}
	configCounts := make(map[string]int)
	attemptCounts := make(map[string]int)
	controlCounts := make(map[string]int)
	for _, config := range status.Configs {
		configCounts[string(config.Status)]++
		for _, attempt := range config.Attempts {
			attemptCounts[string(attempt.Status)]++
		}
		for _, control := range config.Controls {
			controlCounts[string(control.State)]++
		}
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(w, "converge_snapshot_revision %d\nconverge_snapshot_dispatched_revision %d\n", status.Snapshot.Revision, status.DispatchedRevision)
	writeMetricCounts(w, "converge_configs", "status", configCounts)
	writeMetricCounts(w, "converge_attempts", "status", attemptCounts)
	writeMetricCounts(w, "converge_controls", "state", controlCounts)
}

func writeMetricCounts(w io.Writer, metric, label string, counts map[string]int) {
	values := make([]string, 0, len(counts))
	for value := range counts {
		values = append(values, value)
	}
	sort.Strings(values)
	for _, value := range values {
		_, _ = fmt.Fprintf(w, "%s{%s=%q} %d\n", metric, label, value, counts[value])
	}
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return coreerrors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
