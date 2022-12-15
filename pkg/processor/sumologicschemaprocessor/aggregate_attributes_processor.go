// Copyright 2022 Sumo Logic, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sumologicschemaprocessor

import (
	"regexp"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// aggregateAttributesProcessor
type aggregateAttributesProcessor struct {
	aggregations []*aggregation
}

type aggregation struct {
	attribute      string
	patternRegexes []*regexp.Regexp
}

func newAggregateAttributesProcessor(config []aggregationPair) (*aggregateAttributesProcessor, error) {
	aggregations := []*aggregation{}

	for i := 0; i < len(config); i++ {
		pair, err := pairToAggregation(&config[i])
		if err != nil {
			return nil, err
		}
		aggregations = append(aggregations, pair)
	}

	return &aggregateAttributesProcessor{aggregations: aggregations}, nil
}

func pairToAggregation(pair *aggregationPair) (*aggregation, error) {
	regexes := []*regexp.Regexp{}

	for i := 0; i < len(pair.Patterns); i++ {
		// We do not support regexes - only wildcards (*). Escape all regex special characters.
		regexStr := regexp.QuoteMeta(pair.Patterns[i])

		// Replace all wildcards (after escaping they are "\*") with grouped regex wildcard ("(.*)")
		regexStrWithWildcard := strings.Replace(regexStr, "\\*", "(.*)", -1)

		regex, err := regexp.Compile(regexStrWithWildcard)
		if err != nil {
			return nil, err
		}

		regexes = append(regexes, regex)
	}

	return &aggregation{attribute: pair.Attribute, patternRegexes: regexes}, nil
}

func (proc *aggregateAttributesProcessor) processLogs(logs plog.Logs) error {
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		proc.aggregateAttributes(logs.ResourceLogs().At(i).Resource().Attributes())
	}
	return nil
}

func (proc *aggregateAttributesProcessor) processMetrics(metrics pmetric.Metrics) error {
	for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
		proc.aggregateAttributes(metrics.ResourceMetrics().At(i).Resource().Attributes())
	}
	return nil
}

func (proc *aggregateAttributesProcessor) processTraces(traces ptrace.Traces) error {
	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		proc.aggregateAttributes(traces.ResourceSpans().At(i).Resource().Attributes())
	}
	return nil
}

func (proc *aggregateAttributesProcessor) isEnabled() bool {
	return len(proc.aggregations) != 0
}

func (*aggregateAttributesProcessor) ConfigPropertyName() string {
	return "aggregate_attributes"
}

func (proc *aggregateAttributesProcessor) aggregateAttributes(attributes pcommon.Map) {

}
