// Package metrics provides a bounded, dependency-free Prometheus exposition.
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Histogram struct {
	Count uint64
	Sum   float64
}

type labeledKey struct {
	Name  string
	Label string
	Value string
}

type Registry struct {
	mu              sync.Mutex
	counters        map[string]uint64
	labeledCounters map[labeledKey]uint64
	gauges          map[string]float64
	labeledGauges   map[labeledKey]float64
	histograms      map[string]Histogram
}

func New() *Registry {
	return &Registry{
		counters: make(map[string]uint64), labeledCounters: make(map[labeledKey]uint64),
		gauges: make(map[string]float64), labeledGauges: make(map[labeledKey]float64), histograms: make(map[string]Histogram),
	}
}

func (r *Registry) Inc(name string) {
	r.mu.Lock()
	r.counters[name]++
	r.mu.Unlock()
}

func (r *Registry) Add(name string, value uint64) {
	r.mu.Lock()
	r.counters[name] += value
	r.mu.Unlock()
}

func (r *Registry) IncLabel(name, label, value string) {
	r.mu.Lock()
	r.labeledCounters[labeledKey{Name: name, Label: label, Value: value}]++
	r.mu.Unlock()
}

func (r *Registry) Set(name string, value float64) {
	r.mu.Lock()
	r.gauges[name] = value
	r.mu.Unlock()
}

func (r *Registry) SetLabel(name, label, value string, number float64) {
	r.mu.Lock()
	r.labeledGauges[labeledKey{Name: name, Label: label, Value: value}] = number
	r.mu.Unlock()
}

func (r *Registry) Observe(name string, value float64) {
	r.mu.Lock()
	h := r.histograms[name]
	h.Count++
	h.Sum += value
	r.histograms[name] = h
	r.mu.Unlock()
}

func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.mu.Lock()
		defer r.mu.Unlock()
		names := make([]string, 0, len(r.counters))
		for name := range r.counters {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			fmt.Fprintf(w, "%s %d\n", sanitize(name), r.counters[name])
		}

		labelKeys := make([]labeledKey, 0, len(r.labeledCounters))
		for key := range r.labeledCounters {
			labelKeys = append(labelKeys, key)
		}
		sort.Slice(labelKeys, func(i, j int) bool { return formatKey(labelKeys[i]) < formatKey(labelKeys[j]) })
		for _, key := range labelKeys {
			fmt.Fprintf(w, "%s %d\n", formatKey(key), r.labeledCounters[key])
		}

		names = names[:0]
		for name := range r.gauges {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			fmt.Fprintf(w, "%s %g\n", sanitize(name), r.gauges[name])
		}

		labelKeys = labelKeys[:0]
		for key := range r.labeledGauges {
			labelKeys = append(labelKeys, key)
		}
		sort.Slice(labelKeys, func(i, j int) bool { return formatKey(labelKeys[i]) < formatKey(labelKeys[j]) })
		for _, key := range labelKeys {
			fmt.Fprintf(w, "%s %g\n", formatKey(key), r.labeledGauges[key])
		}

		names = names[:0]
		for name := range r.histograms {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			h := r.histograms[name]
			fmt.Fprintf(w, "%s_count %d\n%s_sum %g\n", sanitize(name), h.Count, sanitize(name), h.Sum)
		}
	})
}

func formatKey(key labeledKey) string {
	return sanitize(key.Name) + "{" + sanitize(key.Label) + "=" + strconv.Quote(key.Value) + "}"
}

func sanitize(name string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' {
			return r
		}
		return '_'
	}, name)
}
