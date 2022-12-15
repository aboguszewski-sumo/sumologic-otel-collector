package sumologicschemaprocessor

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestAggregation(t *testing.T) {
	testCases := []struct {
		name         string
		input        map[string]pcommon.Value
		expected     map[string]pcommon.Value
		aggregations []*aggregation
	}{
		{
			name: "three values one key",
			input: map[string]pcommon.Value{
				"pod_first":  pcommon.NewValueStr("first"),
				"pod_second": pcommon.NewValueStr("second"),
				"pod_third":  pcommon.NewValueStr("third"),
			},
			expected: map[string]pcommon.Value{
				"pods": mapToPcommonValue(map[string]pcommon.Value{
					"first":  pcommon.NewValueStr("first"),
					"second": pcommon.NewValueStr("second"),
					"third":  pcommon.NewValueStr("third"),
				}),
			},
			aggregations: []*aggregation{
				{
					attribute: "pods",
					patternRegexes: []*regexp.Regexp{
						regexp.MustCompile("pod_(.*)"),
					},
				},
			},
		},
		{
			name: "two wildcards",
			input: map[string]pcommon.Value{
				"cool_pod_first":  pcommon.NewValueStr("first"),
				"fine_pod_second": pcommon.NewValueStr("second"),
				"nice_pod_third":  pcommon.NewValueStr("third"),
			},
			expected: map[string]pcommon.Value{
				"pods": mapToPcommonValue(map[string]pcommon.Value{
					"cool_first":  pcommon.NewValueStr("first"),
					"fine_second": pcommon.NewValueStr("second"),
					"nice_third":  pcommon.NewValueStr("third"),
				}),
			},
			aggregations: []*aggregation{
				{
					attribute: "pods",
					patternRegexes: []*regexp.Regexp{
						regexp.MustCompile("(.*)_pod_(.*)"),
					},
				},
			},
		},
		{
			name: "six values two keys",
			input: map[string]pcommon.Value{
				"pod_first":                pcommon.NewValueStr("first"),
				"pod_second":               pcommon.NewValueStr("second"),
				"pod_third":                pcommon.NewValueStr("third"),
				"sono_ichi":                pcommon.NewValueInt(1),
				"sono_ni":                  pcommon.NewValueInt(2),
				"a totally unrelevant key": pcommon.NewValueBool(true),
			},
			expected: map[string]pcommon.Value{
				"pods": mapToPcommonValue(map[string]pcommon.Value{
					"first":  pcommon.NewValueStr("first"),
					"second": pcommon.NewValueStr("second"),
					"third":  pcommon.NewValueStr("third"),
				}),
				"counts": mapToPcommonValue(map[string]pcommon.Value{
					"ichi": pcommon.NewValueInt(1),
					"ni":   pcommon.NewValueInt(2),
				}),
				"a totally unrelevant key": pcommon.NewValueBool(true),
			},
			aggregations: []*aggregation{
				{
					attribute: "pods",
					patternRegexes: []*regexp.Regexp{
						regexp.MustCompile("pod_(.*)"),
					},
				},
				{
					attribute: "counts",
					patternRegexes: []*regexp.Regexp{
						regexp.MustCompile("sono_(.*)"),
					},
				},
			},
		},
		{
			name: "overlapping regexes in different aggregations",
			input: map[string]pcommon.Value{
				"pod_clone_zone_throne_gnome": pcommon.NewValueStr("a"),
				"pod_clone_zone_crown_gnome":  pcommon.NewValueStr("b"),
				"pod_frown_zone_throne_gnome": pcommon.NewValueStr("c"),
			},
			expected: map[string]pcommon.Value{
				"clones": mapToPcommonValue(map[string]pcommon.Value{
					"throne": pcommon.NewValueStr("a"),
					"crown":  pcommon.NewValueStr("b"),
				}),
				"thrones": mapToPcommonValue(map[string]pcommon.Value{
					"frown": pcommon.NewValueStr("c"),
				}),
			},
			aggregations: []*aggregation{
				{
					attribute: "clones",
					patternRegexes: []*regexp.Regexp{
						regexp.MustCompile("pod_clone_zone_(.*)_gnome"),
					},
				},
				{
					attribute: "thrones",
					patternRegexes: []*regexp.Regexp{
						regexp.MustCompile("pod_(.*)_zone_throne_gnome"),
					},
				},
			},
		},
		{
			name: "overlapping regexes in one aggregation",
			input: map[string]pcommon.Value{
				"pod_clone_zone_throne_gnome": pcommon.NewValueStr("a"),
				"pod_clone_zone_crown_gnome":  pcommon.NewValueStr("b"),
				"pod_frown_zone_throne_gnome": pcommon.NewValueStr("c"),
			},
			expected: map[string]pcommon.Value{
				"zones": mapToPcommonValue(map[string]pcommon.Value{
					"throne": pcommon.NewValueStr("a"),
					"crown":  pcommon.NewValueStr("b"),
					"frown":  pcommon.NewValueStr("c"),
				}),
			},
			aggregations: []*aggregation{
				{
					attribute: "zones",
					patternRegexes: []*regexp.Regexp{
						regexp.MustCompile("pod_clone_zone_(.*)_gnome"),
						regexp.MustCompile("pod_(.*)_zone_throne_gnome"),
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			processor := aggregateAttributesProcessor{
				aggregations: testCase.aggregations,
			}

			attrs := mapToPcommonMap(testCase.input)

			err := processor.processAttributes(attrs)
			require.NoError(t, err)

			expected := mapToPcommonMap(testCase.expected)

			require.Equal(t, expected.AsRaw(), attrs.AsRaw())
		})
	}
}

func TestMetrics(t *testing.T) {
	aggregations := []*aggregation{{
		attribute:      "a",
		patternRegexes: []*regexp.Regexp{regexp.MustCompile("b_(.*)")},
	}}
	testCases := []struct {
		name         string
		createMetric func() pmetric.Metric
		test         func(pmetric.Metric)
	}{
		{
			name: "empty",
			createMetric: func() pmetric.Metric {
				return pmetric.NewMetric()
			},
			test: func(m pmetric.Metric) {
				require.Equal(t, m.Type(), pmetric.MetricTypeEmpty)
			},
		},
		{
			name: "sum",
			createMetric: func() pmetric.Metric {
				m := pmetric.NewMetric()
				s := m.SetEmptySum()
				s.DataPoints().AppendEmpty().Attributes().PutStr("b_c", "x")

				return m
			},
			test: func(m pmetric.Metric) {
				s := pmetric.NewMetric().SetEmptySum()
				s.DataPoints().AppendEmpty().Attributes().PutEmptyMap("a").PutStr("c", "x")

				require.Equal(t, m.Type(), pmetric.MetricTypeSum)
				require.Equal(t, s.DataPoints().At(0).Attributes().AsRaw(), m.Sum().DataPoints().At(0).Attributes().AsRaw())
			},
		},
		{
			name: "gauge",
			createMetric: func() pmetric.Metric {
				m := pmetric.NewMetric()
				s := m.SetEmptyGauge()
				s.DataPoints().AppendEmpty().Attributes().PutStr("b_c", "x")

				return m
			},
			test: func(m pmetric.Metric) {
				s := pmetric.NewMetric().SetEmptyGauge()
				s.DataPoints().AppendEmpty().Attributes().PutEmptyMap("a").PutStr("c", "x")

				require.Equal(t, m.Type(), pmetric.MetricTypeGauge)
				require.Equal(t, s.DataPoints().At(0).Attributes().AsRaw(), m.Gauge().DataPoints().At(0).Attributes().AsRaw())
			},
		},
		{
			name: "histogram",
			createMetric: func() pmetric.Metric {
				m := pmetric.NewMetric()
				s := m.SetEmptyHistogram()
				s.DataPoints().AppendEmpty().Attributes().PutStr("b_c", "x")

				return m
			},
			test: func(m pmetric.Metric) {
				s := pmetric.NewMetric().SetEmptyHistogram()
				s.DataPoints().AppendEmpty().Attributes().PutEmptyMap("a").PutStr("c", "x")

				require.Equal(t, m.Type(), pmetric.MetricTypeHistogram)
				require.Equal(t, s.DataPoints().At(0).Attributes().AsRaw(), m.Histogram().DataPoints().At(0).Attributes().AsRaw())
			},
		},
		{
			name: "exponential histogram",
			createMetric: func() pmetric.Metric {
				m := pmetric.NewMetric()
				s := m.SetEmptyExponentialHistogram()
				s.DataPoints().AppendEmpty().Attributes().PutStr("b_c", "x")

				return m
			},
			test: func(m pmetric.Metric) {
				s := pmetric.NewMetric().SetEmptyExponentialHistogram()
				s.DataPoints().AppendEmpty().Attributes().PutEmptyMap("a").PutStr("c", "x")

				require.Equal(t, m.Type(), pmetric.MetricTypeExponentialHistogram)
				require.Equal(t, s.DataPoints().At(0).Attributes().AsRaw(), m.ExponentialHistogram().DataPoints().At(0).Attributes().AsRaw())
			},
		},
		{
			name: "summary",
			createMetric: func() pmetric.Metric {
				m := pmetric.NewMetric()
				s := m.SetEmptySummary()
				s.DataPoints().AppendEmpty().Attributes().PutStr("b_c", "x")

				return m
			},
			test: func(m pmetric.Metric) {
				s := pmetric.NewMetric().SetEmptySummary()
				s.DataPoints().AppendEmpty().Attributes().PutEmptyMap("a").PutStr("c", "x")

				require.Equal(t, m.Type(), pmetric.MetricTypeSummary)
				require.Equal(t, s.DataPoints().At(0).Attributes().AsRaw(), m.Summary().DataPoints().At(0).Attributes().AsRaw())
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			processor := aggregateAttributesProcessor{
				aggregations: aggregations,
			}

			metric := testCase.createMetric()
			err := processMetricLevelAttributes(&processor, metric)
			require.NoError(t, err)

			testCase.test(metric)
		})
	}
}
