package fake

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	topopb "github.com/google/kne/proto/topo"
	"github.com/google/kne/topo/node"
	"github.com/h-fam/errdiff"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestNew(t *testing.T) {
	tests := []struct {
		desc    string
		ni      *node.Impl
		wantPB  *topopb.Node
		wantErr string
	}{{
		desc:    "nil node impl",
		wantErr: "nodeImpl cannot be nil",
	}, {
		desc:    "nil pb",
		wantErr: "nodeImpl.Proto cannot be nil",
		ni:      &node.Impl{},
	}, {
		desc: "test defaults",
		ni: &node.Impl{
			Proto: &topopb.Node{
				Name: "test_node",
				Config: &topopb.Config{
					Image:   "foobar",
					Command: []string{"run", "some", "command"},
				},
			},
		},
		wantPB: &topopb.Node{
			Name: "test_node",
			Config: &topopb.Config{
				Image:        "foobar",
				Command:      []string{"run", "some", "command"},
				Args:         []string{"-target", "test_node", "-port", "6030"},
				EntryCommand: "kubectl exec -it test_node -- /bin/bash",
			},
			Services: map[uint32]*topopb.Service{
				6030: {
					Name:     "gnmi",
					Inside:   6030,
					NodePort: 30001,
				},
			},
		},
	}, {
		desc: "valid pb",
		ni: &node.Impl{
			Proto: &topopb.Node{
				Name: "test_node",
			},
		},
		wantPB: &topopb.Node{
			Name: "test_node",
			Config: &topopb.Config{
				Image:        "wenovus/fakeserver0",
				EntryCommand: "kubectl exec -it test_node -- /bin/bash",
				Args:         []string{"-target", "test_node", "-port", "6030"},
			},
			Services: map[uint32]*topopb.Service{
				6030: {
					Name:     "gnmi",
					Inside:   6030,
					NodePort: 30002,
				},
			},
		},
	}}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			impl, err := New(tt.ni)
			if s := errdiff.Substring(err, tt.wantErr); s != "" {
				t.Fatalf("unexpected error: got: %v, want: %s", err, s)
			}
			if tt.wantErr != "" {
				return
			}
			if diff := cmp.Diff(impl.GetProto(), tt.wantPB, protocmp.Transform()); diff != "" {
				t.Fatalf("New() failed: (-got, +want)%s", diff)
			}
		})
	}
}
