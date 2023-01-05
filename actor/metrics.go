// Copyright (C) 2017 - 2022 Asynkron.se <http://www.asynkron.se>

package actor

import (
	"container/list"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/asynkron/protoactor-go/log"

	"github.com/asynkron/protoactor-go/extensions"
	"github.com/asynkron/protoactor-go/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/unit"
)

var extensionId = extensions.NextExtensionID()

type Metrics struct {
	metrics *metrics.ProtoMetrics
	enabled bool
}

var _ extensions.Extension = &Metrics{}

func (m *Metrics) Enabled() bool {
	return m.enabled
}

func (m *Metrics) ExtensionID() extensions.ExtensionID {
	return extensionId
}

func NewMetrics(provider metric.MeterProvider) *Metrics {
	if provider == nil {
		return &Metrics{}
	}

	m := &Metrics{
		metrics: metrics.NewProtoMetrics(provider),
		enabled: true,
	}

	if instruments := m.metrics.Get(metrics.InternalActorMetrics); instruments != nil {
		m.PrepareMailboxLengthGauge()
		meter := global.Meter(metrics.LibName)
		if err := meter.RegisterCallback([]instrument.Asynchronous{instruments.ActorMailboxLength}, func(goCtx context.Context) {
			var start = time.Now()
			i := 0
			deleted := 0
			for _, mbsProvider := range mailboxSizeProviders {
				var count int64 = 0
				mbsProvider.Lock()
				for e := mbsProvider.invokers.Front(); e != nil; {
					if c, dead := e.Value.(mailboxSizeInvoker)(); dead {
						deadElem := e
						e = e.Next() // needs to be before Remove
						mbsProvider.invokers.Remove(deadElem)
						deleted++
					} else {
						count += int64(c)
						e = e.Next()
					}
					i++
				}
				mbsProvider.Unlock()
				instruments.ActorMailboxLength.Observe(goCtx, count, mbsProvider.labels...)
			}
			fmt.Printf("elapsed: %v, providers: %v, iterated: %v, deleted: %v\n",
				time.Since(start), len(mailboxSizeProviders), i, deleted)

		}); err != nil {
			err = fmt.Errorf("failed to instrument Actor Mailbox, %w", err)
			plog.Error(err.Error(), log.Error(err))
		}
	}

	return m
}

func (m *Metrics) PrepareMailboxLengthGauge() {
	meter := global.Meter(metrics.LibName)
	gauge, err := meter.AsyncInt64().Gauge("protoactor_actor_mailbox_length",
		instrument.WithDescription("Actor's Mailbox Length"),
		instrument.WithUnit(unit.Dimensionless))

	if err != nil {
		err = fmt.Errorf("failed to create ActorMailBoxLength instrument, %w", err)
		plog.Error(err.Error(), log.Error(err))
	}
	m.metrics.Instruments().SetActorMailboxLengthGauge(gauge)
}

func (m *Metrics) CommonLabels(ctx Context) []attribute.KeyValue {
	labels := []attribute.KeyValue{
		attribute.String("address", ctx.ActorSystem().Address()),
		attribute.String("actortype", strings.Replace(fmt.Sprintf("%T", ctx.Actor()), "*", "", 1)),
	}

	return labels
}

type mailboxSizeInvoker func() (mailboxSize int, dead bool)

var mailboxSizeProviders = make(map[string]*mailboxSizeProvider)

type mailboxSizeProvider struct {
	labels   []attribute.KeyValue
	invokers *list.List
	sync.RWMutex
}

func registerMailboxSizeProvider(invoker mailboxSizeInvoker, labels []attribute.KeyValue) {
	labelsAsString := labelsToString(labels)
	var provider *mailboxSizeProvider
	if p, ok := mailboxSizeProviders[labelsAsString]; ok {
		provider = p
	} else {
		provider = &mailboxSizeProvider{labels: labels, invokers: list.New()}
		mailboxSizeProviders[labelsAsString] = provider
	}
	provider.Lock()
	provider.invokers.PushBack(invoker)
	provider.Unlock()
}

func labelsToString(labels []attribute.KeyValue) string {
	sb := strings.Builder{}
	sort.Slice(labels, func(i, j int) bool {
		return labels[i].Key < labels[j].Key
	})
	for _, label := range labels {
		sb.WriteString(string(label.Key))
		sb.WriteRune('=')
		sb.WriteString(label.Value.Emit())
		sb.WriteRune(',')
	}
	return sb.String()
}
