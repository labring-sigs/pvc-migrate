package crosscluster_test

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	. "github.com/labring-sigs/pvc-migrate/internal/crosscluster"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func sessionConfigMapName(id string) string {
	return "pvc-migrate-cross-cluster-" + id
}

// assignConfigMapResourceVersions models the API server behavior the fake
// clientset omits: every persisted ConfigMap write receives a new
// resourceVersion. The record guards compare resource versions, so tests that
// pin them need writes to be versioned like production.
func assignConfigMapResourceVersions(client *fake.Clientset) {
	var version atomic.Int64

	assign := func(action clienttesting.Action) (bool, runtime.Object, error) {
		configMap, err := testutil.ActionObject[*corev1.ConfigMap](action)
		if err != nil {
			return true, nil, err
		}

		configMap.ResourceVersion = strconv.FormatInt(version.Add(1), 10)

		return false, nil, nil
	}

	client.PrependReactor("create", "configmaps", assign)
	client.PrependReactor("update", "configmaps", assign)
}

func mustGetSessionConfigMap(
	t *testing.T,
	client *fake.Clientset,
	namespace, id string,
) *corev1.ConfigMap {
	t.Helper()

	cm, err := client.CoreV1().ConfigMaps(namespace).
		Get(t.Context(), sessionConfigMapName(id), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read session ConfigMap: %v", err)
	}

	return cm
}

// TestCopySessionDeleteRefusesReplacedOrForeignRecords pins the record-delete
// guards: a cleanup may only remove the exact ConfigMap record it owns.
func TestCopySessionDeleteRefusesReplacedOrForeignRecords(t *testing.T) {
	cases := []struct {
		name    string
		tamper  func(t *testing.T, client *fake.Clientset, cm *corev1.ConfigMap)
		message string
	}{
		{
			name: "ownership label removed",
			tamper: func(t *testing.T, client *fake.Clientset, cm *corev1.ConfigMap) {
				t.Helper()

				delete(cm.Labels, ManagedByLabel)

				if _, err := client.CoreV1().ConfigMaps(cm.Namespace).
					Update(t.Context(), cm, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			},
			message: "ownership changed",
		},
		{
			name: "payload carries a different session",
			tamper: func(t *testing.T, client *fake.Clientset, cm *corev1.ConfigMap) {
				t.Helper()

				var replaced CopySession
				if err := json.Unmarshal([]byte(cm.Data["session.json"]), &replaced); err != nil {
					t.Fatal(err)
				}

				replaced.ID = "another-session"

				raw, err := json.Marshal(replaced)
				if err != nil {
					t.Fatal(err)
				}

				cm.Data = map[string]string{"session.json": string(raw)}

				if _, err := client.CoreV1().ConfigMaps(cm.Namespace).
					Update(t.Context(), cm, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			},
			message: "contents do not match",
		},
		{
			name: "record was recreated under the same name",
			tamper: func(t *testing.T, client *fake.Clientset, cm *corev1.ConfigMap) {
				t.Helper()

				replacement := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      cm.Name,
						Namespace: cm.Namespace,
						Labels: map[string]string{
							ManagedByLabel: ManagedBy,
							SessionKey:     cm.Labels[SessionKey],
						},
					},
					Data: cm.Data,
				}

				if err := client.CoreV1().ConfigMaps(cm.Namespace).
					Delete(t.Context(), cm.Name, metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}

				if _, err := client.CoreV1().ConfigMaps(cm.Namespace).
					Create(t.Context(), replacement, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			},
			message: "changed while deleting",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			service, options, _ := crossFixture()

			source, ok := service.SourceClientForTest().(*fake.Clientset)
			if !ok {
				t.Fatalf("source client type=%T", service.SourceClientForTest())
			}

			assignConfigMapResourceVersions(source)

			plan, err := service.PlanCopy(t.Context(), options)
			if err != nil || !plan.Ready {
				t.Fatalf("plan ready=%v err=%v checks=%#v", plan.Ready, err, plan.Checks)
			}

			session, err := service.CreateCopySession(t.Context(), options, plan)
			if err != nil {
				t.Fatal(err)
			}

			testCase.tamper(
				t,
				source,
				mustGetSessionConfigMap(t, source, options.SessionNamespace, session.ID),
			)

			if err := service.DeleteSessionRecordForTest(t.Context(), session); err == nil ||
				!strings.Contains(err.Error(), testCase.message) {
				t.Fatalf("delete error=%v, want guard %q", err, testCase.message)
			}

			mustGetSessionConfigMap(t, source, options.SessionNamespace, session.ID)
		})
	}
}

// TestReservationSessionDeleteRefusesReplacedOrForeignRecords pins the same
// record-delete guards for the reservation session store.
func TestReservationSessionDeleteRefusesReplacedOrForeignRecords(t *testing.T) {
	cases := []struct {
		name    string
		tamper  func(t *testing.T, client *fake.Clientset, cm *corev1.ConfigMap)
		message string
	}{
		{
			name: "ownership label removed",
			tamper: func(t *testing.T, client *fake.Clientset, cm *corev1.ConfigMap) {
				t.Helper()

				delete(cm.Labels, ManagedByLabel)

				if _, err := client.CoreV1().ConfigMaps(cm.Namespace).
					Update(t.Context(), cm, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			},
			message: "ownership changed",
		},
		{
			name: "payload replaced with a copy session",
			tamper: func(t *testing.T, client *fake.Clientset, cm *corev1.ConfigMap) {
				t.Helper()

				var replaced ReservationSession
				if err := json.Unmarshal([]byte(cm.Data["session.json"]), &replaced); err != nil {
					t.Fatal(err)
				}

				replaced.Kind = CopyKind

				raw, err := json.Marshal(replaced)
				if err != nil {
					t.Fatal(err)
				}

				cm.Data = map[string]string{"session.json": string(raw)}

				if _, err := client.CoreV1().ConfigMaps(cm.Namespace).
					Update(t.Context(), cm, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			},
			message: "contents do not match",
		},
		{
			name: "record was recreated under the same name",
			tamper: func(t *testing.T, client *fake.Clientset, cm *corev1.ConfigMap) {
				t.Helper()

				replacement := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      cm.Name,
						Namespace: cm.Namespace,
						Labels: map[string]string{
							ManagedByLabel: ManagedBy,
							SessionKey:     cm.Labels[SessionKey],
						},
					},
					Data: cm.Data,
				}

				if err := client.CoreV1().ConfigMaps(cm.Namespace).
					Delete(t.Context(), cm.Name, metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}

				if _, err := client.CoreV1().ConfigMaps(cm.Namespace).
					Create(t.Context(), replacement, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			},
			message: "changed while deleting",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			service, options, reservation := reservationFixture(t)

			source, ok := service.SourceClientForTest().(*fake.Clientset)
			if !ok {
				t.Fatalf("source client type=%T", service.SourceClientForTest())
			}

			testCase.tamper(
				t,
				source,
				mustGetSessionConfigMap(t, source, options.SessionNamespace, reservation.ID),
			)

			if err := service.DeleteReservationRecordForTest(
				t.Context(),
				reservation,
			); err == nil ||
				!strings.Contains(err.Error(), testCase.message) {
				t.Fatalf("delete error=%v, want guard %q", err, testCase.message)
			}

			mustGetSessionConfigMap(t, source, options.SessionNamespace, reservation.ID)
		})
	}
}

func injectFirstRecordDeleteFailure(t *testing.T, client *fake.Clientset, sessionID string) {
	t.Helper()

	var failures atomic.Int32

	client.PrependReactor(
		"delete",
		"configmaps",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			deleteAction, ok := action.(clienttesting.DeleteAction)
			if !ok || deleteAction.GetName() != sessionConfigMapName(sessionID) {
				return false, nil, nil
			}

			if failures.Add(1) == 1 {
				return true, nil, errors.New("injected session record delete failure")
			}

			return false, nil, nil
		},
	)
}

// TestCleanupRetryReacquiresLeaseAfterRecordDeleteFailure covers the window
// where session teardown deletes the fencing Lease but the ConfigMap record
// delete fails: the record must survive and a retry must re-acquire the Lease
// and finish the deletion.
func TestCleanupRetryReacquiresLeaseAfterRecordDeleteFailure(t *testing.T) {
	service, options, _ := crossFixture()

	source, ok := service.SourceClientForTest().(*fake.Clientset)
	if !ok {
		t.Fatalf("source client type=%T", service.SourceClientForTest())
	}

	assignConfigMapResourceVersions(source)

	plan, err := service.PlanCopy(t.Context(), options)
	if err != nil || !plan.Ready {
		t.Fatalf("plan ready=%v err=%v checks=%#v", plan.Ready, err, plan.Checks)
	}

	session, err := service.CreateCopySession(t.Context(), options, plan)
	if err != nil {
		t.Fatal(err)
	}

	injectFirstRecordDeleteFailure(t, source, session.ID)

	first := service.Cleanup(t.Context(), session, "Keep", true)
	if first == nil || !strings.Contains(first.Error(), "injected session record delete failure") {
		t.Fatalf("first cleanup error=%v, want injected record delete failure", first)
	}

	// The Lease was removed first, so the stale holder cannot fence a retry.
	_, err = source.CoordinationV1().Leases(options.SessionNamespace).
		Get(t.Context(), kube.SessionLockName(session.ID), metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("lease after failed record deletion: err=%v, want not found", err)
	}

	// The record itself must remain for the retry.
	mustGetSessionConfigMap(t, source, options.SessionNamespace, session.ID)

	if err := service.Cleanup(t.Context(), session, "Keep", true); err != nil {
		t.Fatalf("cleanup retry did not converge: %v", err)
	}

	_, err = source.CoreV1().ConfigMaps(options.SessionNamespace).
		Get(t.Context(), sessionConfigMapName(session.ID), metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("session record after retry: err=%v, want not found", err)
	}

	_, err = source.CoordinationV1().Leases(options.SessionNamespace).
		Get(t.Context(), kube.SessionLockName(session.ID), metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("lease after retry: err=%v, want not found", err)
	}
}

// TestCleanupReservationRetryConvergesAfterRecordDeleteFailure covers the same
// teardown window for cross-cluster reservation sessions.
func TestCleanupReservationRetryConvergesAfterRecordDeleteFailure(t *testing.T) {
	service, options, reservation := reservationFixture(t)

	source, ok := service.SourceClientForTest().(*fake.Clientset)
	if !ok {
		t.Fatalf("source client type=%T", service.SourceClientForTest())
	}

	injectFirstRecordDeleteFailure(t, source, reservation.ID)

	first := service.CleanupReservation(t.Context(), reservation, "Keep", true)
	if first == nil || !strings.Contains(first.Error(), "injected session record delete failure") {
		t.Fatalf("first cleanup error=%v, want injected record delete failure", first)
	}

	_, err := source.CoordinationV1().Leases(options.SessionNamespace).
		Get(t.Context(), kube.SessionLockName(reservation.ID), metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("lease after failed record deletion: err=%v, want not found", err)
	}

	mustGetSessionConfigMap(t, source, options.SessionNamespace, reservation.ID)

	if err := service.CleanupReservation(t.Context(), reservation, "Keep", true); err != nil {
		t.Fatalf("cleanup retry did not converge: %v", err)
	}

	_, err = source.CoreV1().ConfigMaps(options.SessionNamespace).
		Get(t.Context(), sessionConfigMapName(reservation.ID), metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("reservation record after retry: err=%v, want not found", err)
	}

	_, err = source.CoordinationV1().Leases(options.SessionNamespace).
		Get(t.Context(), kube.SessionLockName(reservation.ID), metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("lease after retry: err=%v, want not found", err)
	}
}
