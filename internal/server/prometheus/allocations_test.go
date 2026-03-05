// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package prometheus

import (
	"strings"
	"testing"
)

// These functions are used to benchmark different ways of converting
// a gRPC method name from the form "/arke.Producer/Publish" to "arke.Producer.Publish"
// The goal is to find the method that uses the least allocations and is the fastest
// since this will be called on every gRPC request.

func useMethodMap(fullMethod string) {
	_ = methodMap[fullMethod]
}

func useStringPackgeMethods(fullMethod string) {
	fullMethod = strings.TrimPrefix(fullMethod, "/")
	_ = strings.ReplaceAll(fullMethod, "/", ".")
}

func useStringBuilderMethods(fullMethod string) {
	var sb strings.Builder
	sb.Grow(len(fullMethod))
	for i := 0; i < len(fullMethod); i++ {
		c := fullMethod[i]
		if i == 0 && c == '/' {
			continue
		}
		if c == '/' {
			sb.WriteByte('.')
		} else {
			sb.WriteByte(c)
		}
	}
	_ = sb.String()
}

func Benchmark_LimitMethodAllocationsMethodMap(b *testing.B) {
	for grpcMethod := range methodMap {
		for n := 0; n < b.N; n++ {
			useMethodMap(grpcMethod)
		}
	}
}

func Benchmark_LimitMethodAllocationsStringBuilder(b *testing.B) {
	for grpcMethod := range methodMap {
		for n := 0; n < b.N; n++ {
			useStringBuilderMethods(grpcMethod)
		}
	}
}

func Benchmark_LimitMethodAllocations(b *testing.B) {
	for grpcMethod := range methodMap {
		for n := 0; n < b.N; n++ {
			useStringPackgeMethods(grpcMethod)
		}
	}
}
