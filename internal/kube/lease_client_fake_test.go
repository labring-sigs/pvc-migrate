package kube

import (
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func newSessionLeaseTestClient() *fake.Clientset {
	client := fake.NewClientset()
	client.PrependReactor(
		"create",
		"leases",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			lease, err := testutil.ActionObject[*coordinationv1.Lease](action)
			if err != nil {
				return true, nil, err
			}

			if lease.UID == "" {
				lease.UID = types.UID("lease-" + lease.Name)
			}

			return false, nil, nil
		},
	)

	return client
}
