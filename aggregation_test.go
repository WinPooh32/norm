package norm

import (
	"context"
	"reflect"
	"testing"
)

type kint int

func (v kint) Key() int {
	return int(v)
}

type mem struct {
	v []kint
}

func (m mem) Read(ctx context.Context, args struct{}) (value []kint, err error) {
	return []kint(m.v), nil
}

func TestLookup(t *testing.T) {
	type args struct {
		ctx     context.Context
		lhs     Reader[[]kint, struct{}]
		lhsArgs struct{}
		rhs     Reader[[]kint, struct{}]
		rhsArgs struct{}
	}
	tests := []struct {
		name      string
		args      args
		wantMerge []Merge[kint, kint]
		wantErr   bool
	}{
		{
			name: "with matches",
			args: args{
				ctx:     context.Background(),
				lhs:     mem{[]kint{1, 2, 3, 4, 5}},
				lhsArgs: struct{}{},
				rhs:     mem{[]kint{5, 5, 2, 2, 4}},
				rhsArgs: struct{}{},
			},
			wantMerge: []Merge[kint, kint]{
				{1, nil},
				{2, []kint{2, 2}},
				{3, nil},
				{4, []kint{4}},
				{5, []kint{5, 5}},
			},
			wantErr: false,
		},
		{
			name: "no matches",
			args: args{
				ctx:     context.Background(),
				lhs:     mem{[]kint{1, 2, 3, 4, 5}},
				lhsArgs: struct{}{},
				rhs:     mem{[]kint{10, 11, 12, 13, 14}},
				rhsArgs: struct{}{},
			},
			wantMerge: []Merge[kint, kint]{
				{1, nil},
				{2, nil},
				{3, nil},
				{4, nil},
				{5, nil},
			},
			wantErr: false,
		},
		{
			name: "right is empty",
			args: args{
				ctx:     context.Background(),
				lhs:     mem{[]kint{1, 2, 3, 4, 5}},
				lhsArgs: struct{}{},
				rhs:     mem{[]kint{}},
				rhsArgs: struct{}{},
			},
			wantMerge: []Merge[kint, kint]{
				{1, nil},
				{2, nil},
				{3, nil},
				{4, nil},
				{5, nil},
			},
			wantErr: false,
		},
		{
			name: "right is nil",
			args: args{
				ctx:     context.Background(),
				lhs:     mem{[]kint{1, 2, 3, 4, 5}},
				lhsArgs: struct{}{},
				rhs:     mem{},
				rhsArgs: struct{}{},
			},
			wantMerge: []Merge[kint, kint]{
				{1, nil},
				{2, nil},
				{3, nil},
				{4, nil},
				{5, nil},
			},
			wantErr: false,
		},
		{
			name: "left is empty",
			args: args{
				ctx:     context.Background(),
				lhs:     mem{[]kint{}},
				lhsArgs: struct{}{},
				rhs:     mem{[]kint{1, 2, 3, 4, 5}},
				rhsArgs: struct{}{},
			},
			wantMerge: []Merge[kint, kint]{},
			wantErr:   false,
		},
		{
			name: "left is nil",
			args: args{
				ctx:     context.Background(),
				lhs:     mem{},
				lhsArgs: struct{}{},
				rhs:     mem{[]kint{1, 2, 3, 4, 5}},
				rhsArgs: struct{}{},
			},
			wantMerge: []Merge[kint, kint]{},
			wantErr:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMerge, err := Lookup[
				[]kint, []kint,
				kint, kint,
				struct{}, struct{},
				int,
			](
				tt.args.ctx,
				tt.args.lhs, tt.args.lhsArgs,
				tt.args.rhs, tt.args.rhsArgs,
			)
			if (err != nil) != tt.wantErr {
				t.Errorf("Lookup() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotMerge, tt.wantMerge) {
				t.Errorf("Lookup() = %v, want %v", gotMerge, tt.wantMerge)
			}
		})
	}
}

func TestGroup(t *testing.T) {
	type args struct {
		ctx  context.Context
		r    Reader[[]kint, struct{}]
		args struct{}
	}
	tests := []struct {
		name       string
		args       args
		wantGroups map[int][]kint
		wantErr    bool
	}{
		{
			name: "empty values",
			args: args{
				ctx:  context.Background(),
				r:    mem{[]kint{}},
				args: struct{}{},
			},
			wantGroups: map[int][]kint{},
			wantErr:    false,
		},
		{
			name: "nil values",
			args: args{
				ctx:  context.Background(),
				r:    mem{[]kint{}},
				args: struct{}{},
			},
			wantGroups: map[int][]kint{},
			wantErr:    false,
		},
		{
			name: "one element per group",
			args: args{
				ctx:  context.Background(),
				r:    mem{[]kint{1, 2, 3, 4, 5}},
				args: struct{}{},
			},
			wantGroups: map[int][]kint{
				1: {1},
				2: {2},
				3: {3},
				4: {4},
				5: {5},
			},
			wantErr: false,
		},
		{
			name: "several elements per group",
			args: args{
				ctx:  context.Background(),
				r:    mem{[]kint{4, 1, 2, 3, 2, 4, 5, 4, 1, 4, 1}},
				args: struct{}{},
			},
			wantGroups: map[int][]kint{
				1: {1, 1, 1},
				2: {2, 2},
				3: {3},
				4: {4, 4, 4, 4},
				5: {5},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotGroups, err := Group[
				[]kint, kint, struct{}, int,
			](
				tt.args.ctx, tt.args.r, tt.args.args,
			)
			if (err != nil) != tt.wantErr {
				t.Errorf("Group() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotGroups, tt.wantGroups) {
				t.Errorf("Group() = %v, want %v", gotGroups, tt.wantGroups)
			}
		})
	}
}
