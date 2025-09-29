package icanal

import (
	"encoding/hex"
	"reflect"
	"testing"
)

func decodeHexIgnoreError(encoded string) []byte {
	b, _ := hex.DecodeString(encoded)
	return b
}

func Test_scramble411(t *testing.T) {
	type args struct {
		password []byte
		seed     []byte
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "case 1",
			args: args{
				password: []byte("canal"),
				seed:     []byte{238, 30, 253, 112, 26, 247, 36, 63},
			},
			want: decodeHexIgnoreError("97a97da565ef219199cb3ae9bce731c23e7d2d72"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := scramble411(tt.args.password, tt.args.seed); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("scramble411() = %v, want %v", got, tt.want)
			}
		})
	}
}
