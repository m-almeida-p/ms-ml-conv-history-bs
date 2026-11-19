package domain

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidFlow(t *testing.T) {
	tests := []struct {
		name string
		flow string
		want bool
	}{
		{name: "p2p", flow: "p2p", want: true},
		{name: "billPayment", flow: "bill_payment", want: true},
		{name: "largestLength", flow: "a" + strings.Repeat("b", 63), want: true},
		{name: "empty", flow: ""},
		{name: "oneTooLong", flow: "a" + strings.Repeat("b", 64)},
		{name: "startsWithADigit", flow: "2fa"},
		{name: "upperCase", flow: "P2P"},
		{name: "hyphen", flow: "bill-payment"},
		{name: "space", flow: "bill payment"},
		{name: "newline", flow: "p2p\n"},
		{name: "nonASCII", flow: "pagó"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ValidFlow(tt.flow))
		})
	}
}

func TestValidIdentifier(t *testing.T) {
	tests := []struct {
		name string
		id   string
		want bool
	}{
		{name: "bffConversationID", id: "conv_v1_8ab59029-bc3c-4e77-b49f-9fb053d90c52", want: true},
		{name: "accountUUID", id: "896900db-eddd-4c54-99e5-5a32f0830f5e", want: true},
		{name: "allowedPunctuation", id: "a.b_c:d-e", want: true},
		{name: "largestLength", id: strings.Repeat("a", 128), want: true},
		{name: "empty", id: ""},
		{name: "oneTooLong", id: strings.Repeat("a", 129)},
		{name: "space", id: "conv 1"},
		{name: "slash", id: "../etc"},
		{name: "quote", id: "conv'--"},
		{name: "newline", id: "conv\n1"},
		{name: "nonASCII", id: "conv-ü"},
		{name: "nul", id: "a\x00b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ValidIdentifier(tt.id))
		})
	}
}
