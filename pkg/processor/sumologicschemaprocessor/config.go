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
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
)

type Config struct {
	config.ProcessorSettings `mapstructure:",squash"`

	AddCloudNamespace           bool                    `mapstructure:"add_cloud_namespace"`
	TranslateAttributes         bool                    `mapstructure:"translate_attributes"`
	TranslateTelegrafAttributes bool                    `mapstructure:"translate_telegraf_attributes"`
	NestAttributes              *NestingProcessorConfig `mapstructure:"nest_attributes"`
	AggregateAttributes         []aggregationPair       `mapstructure:"aggregate_attributes"`
}

type aggregationPair struct {
	Attribute string   `mapstructure:"attribute"`
	Patterns  []string `mapstructure:"prefixes"`
}

const (
	defaultAddCloudNamespace           = true
	defaultTranslateAttributes         = true
	defaultTranslateTelegrafAttributes = true

	// Nesting processor default config
	defaultNestingEnabled   = false
	defaultNestingSeparator = "."
)

var (
	defaultAggregateAttributes = []aggregationPair{}
)

// Ensure the Config struct satisfies the config.Processor interface.
var (
	_                     component.Config = (*Config)(nil)
	defaultNestingInclude                  = []string{}
	defaultNestingExclude                  = []string{}
)

func createDefaultConfig() component.Config {
	return &Config{
		ProcessorSettings:           config.NewProcessorSettings(component.NewID(typeStr)),
		AddCloudNamespace:           defaultAddCloudNamespace,
		TranslateAttributes:         defaultTranslateAttributes,
		TranslateTelegrafAttributes: defaultTranslateTelegrafAttributes,
		NestAttributes: &NestingProcessorConfig{
			Separator: defaultNestingSeparator,
			Enabled:   defaultNestingEnabled,
			Include:   defaultNestingInclude,
			Exclude:   defaultNestingExclude,
		},
		AggregateAttributes: defaultAggregateAttributes,
	}
}

// Validate config
func (cfg *Config) Validate() error {
	return nil
}
