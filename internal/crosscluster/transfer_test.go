package crosscluster_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/crosscluster"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	kt "k8s.io/client-go/testing"
)

func transferFixture(t *testing.T) (*crosscluster.Service, *crosscluster.CopySession, *fakeCopier) {
	t.Helper()

	service, options, engine := crossFixture()

	plan, err := service.PlanCopy(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}

	session, err := service.CreateCopySession(t.Context(), options, plan)
	if err != nil {
		t.Fatal(err)
	}

	createBoundDestination(t, service, session)

	return service, session, engine
}

type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func seedLocalToolLog(
	t *testing.T,
	client kubernetes.Interface,
	session *crosscluster.CopySession,
	side, line string,
) {
	t.Helper()

	source := session.Spec.Volumes[0].Source.PVC
	identity := copyengine.AttemptIdentity{
		SessionID: session.ID + "-" + source.Name,
		Mode:      copyengine.ModeFinal,
		Attempt:   1,
	}
	identity.Source.Name = source.Name
	identity.Source.Namespace = source.Namespace

	namespace, suffix := source.Namespace, "-src"
	if side == "destination" {
		namespace, suffix = session.Spec.Volumes[0].Destination.PVC.Namespace, "-dest"
	}

	fakeClient := testutil.MustType[*fake.Clientset](t, client)
	fakeClient.PrependReactor("get", "pods", func(a kt.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() == "log" {
			return true, &runtime.Unknown{Raw: []byte(line + "\n")}, nil
		}
		return false, nil, nil
	})

	_, err := client.CoreV1().Pods(namespace).Create(t.Context(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "tool", Namespace: namespace, UID: "tool",
			Labels: map[string]string{
				kube.AppComponentLabel: "sshd",
				kube.AppInstanceLabel: "pv-migrate-" + copyengine.OperationID(
					identity,
				) + "-local" + suffix,
			},
		},
		Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "sshd"}}},
		Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCopyStreamsBothClustersAndCheckpointsBeforeTransfer(t *testing.T) {
	for _, mode := range []string{"text", "json", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			service, session, engine := transferFixture(t)

			var output logBuffer

			structured := mode == "json"
			logger := slog.New(slog.NewJSONHandler(&output, nil))
			service.WithRuntime(
				crosscluster.Runtime{
					Writer:         &output,
					Logger:         logger,
					StreamToolLogs: mode != "disabled",
					StructuredLogs: structured,
				},
			)

			engine.run = func(ctx context.Context, req copyengine.CopyRequest, progress copyengine.ProgressFunc) error {
				saved, err := service.Get(ctx, session.Spec.SessionNamespace, session.ID)
				if err != nil || saved.Status.Volumes[0].Transfer.Attempts != req.Attempt {
					t.Fatalf("attempt not checkpointed: %v, %v", saved, err)
				}

				progress(copyengine.Progress{State: "running", Message: "started transfer"})

				if mode == "disabled" {
					return nil
				}

				deadline := time.Now().Add(3 * time.Second)
				for time.Now().Before(deadline) {
					out := output.String()
					if strings.Contains(out, "source payload") &&
						strings.Contains(out, "destination payload") {
						return nil
					}

					time.Sleep(time.Millisecond)
				}

				return fmt.Errorf("missing tool logs during copy: %s", output.String())
			}

			for side, client := range map[string]kubernetes.Interface{"source": service.SourceClientForTest(), "destination": service.DestinationClientForTest()} {
				seedLocalToolLog(t, client, session, side, side+" payload")
			}

			if err := service.Copy(t.Context(), session, 1, true, ""); err != nil {
				t.Fatal(err)
			}

			out := output.String()
			if !strings.Contains(out, "copy progress") {
				t.Fatal(out)
			}

			switch mode {
			case "disabled":
				for _, client := range []kubernetes.Interface{service.SourceClientForTest(), service.DestinationClientForTest()} {
					for _, a := range testutil.MustType[*fake.Clientset](t, client).Actions() {
						if a.GetSubresource() == "log" ||
							a.GetVerb() == "watch" && a.GetResource().Resource == "pods" {
							t.Fatalf("disabled streaming issued %v", a)
						}
					}
				}
			case "text":
				for _, side := range []string{"source", "destination"} {
					if !strings.Contains(out, "[tool "+side+"/app/tool sshd] "+side+" payload") {
						t.Fatal(out)
					}
				}
			case "json":
				clusters := map[string]bool{}
				for line := range strings.Lines(out) {
					var record map[string]any
					if err := json.Unmarshal([]byte(line), &record); err != nil {
						t.Fatal(err)
					}

					if record["msg"] == "tool Pod log" {
						side, _ := record["cluster"].(string)
						if record["line"] != side+" payload" {
							t.Fatal(record)
						}

						clusters[side] = true
					}
				}

				if !clusters["source"] || !clusters["destination"] {
					t.Fatal(out)
				}
			}
		})
	}
}

func TestCrossCopyAttributesPodCapacityErrorsToDestination(t *testing.T) {
	for _, side := range []string{"source", "destination"} {
		t.Run(side, func(t *testing.T) {
			service, session, engine := transferFixture(t)

			var output logBuffer
			service.WithRuntime(crosscluster.Runtime{Writer: &output, StreamToolLogs: true})

			client := service.SourceClientForTest()
			if side == "destination" {
				client = service.DestinationClientForTest()
			}

			const line = "write failed: no space left on device"
			seedLocalToolLog(t, client, session, side, line)

			engineErr := errors.New("transfer interrupted")
			engine.run = func(ctx context.Context, _ copyengine.CopyRequest, _ copyengine.ProgressFunc) error {
				for !strings.Contains(output.String(), line) {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(time.Millisecond):
					}
				}

				return engineErr
			}

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			err := service.Copy(ctx, session, 1, true, "")
			if !errors.Is(err, engineErr) ||
				errors.Is(err, kube.ErrToolPodNoSpace) != (side == "destination") {
				t.Fatalf("%s log produced %v", side, err)
			}
		})
	}
}

func TestCrossClusterRetryBoundaries(t *testing.T) {
	for _, kind := range []string{"timeout", "transient", "capacity", "cleanup failure", "checkpoint failure"} {
		t.Run(kind, func(t *testing.T) {
			service, session, engine := transferFixture(t)

			var out logBuffer

			const backoff = 15 * time.Millisecond
			service.WithRuntime(
				crosscluster.Runtime{
					Logger:       slog.New(slog.NewTextHandler(&out, nil)),
					CopyTimeout:  50 * time.Millisecond,
					RetryBackoff: backoff,
				},
			)

			var first time.Time

			engine.run = func(ctx context.Context, req copyengine.CopyRequest, _ copyengine.ProgressFunc) error {
				if req.Attempt == 2 {
					if time.Since(first) < backoff {
						t.Fatal("missing retry backoff")
					}
					return nil
				}

				first = time.Now()

				switch kind {
				case "timeout":
					<-ctx.Done()
					return ctx.Err()
				case "capacity":
					return errors.New("rsync: no space left on device")
				default:
					return errors.New("transfer interrupted")
				}
			}

			if kind == "cleanup failure" {
				c := testutil.MustType[*fake.Clientset](t, service.DestinationClientForTest())
				c.PrependReactor(
					"list",
					"deployments",
					func(kt.Action) (bool, runtime.Object, error) {
						return true, nil, errors.New("cannot confirm release")
					},
				)
			}

			if kind == "checkpoint failure" {
				c := testutil.MustType[*fake.Clientset](t, service.SourceClientForTest())
				c.PrependReactor(
					"update",
					"configmaps",
					func(a kt.Action) (bool, runtime.Object, error) {
						obj := testutil.MustActionObject[*corev1.ConfigMap](t, a)

						var saved crosscluster.CopySession
						if err := json.Unmarshal(
							[]byte(obj.Data["session.json"]),
							&saved,
						); err != nil {
							return true, nil, err
						}

						if saved.Status.Volumes[0].Transfer.Attempts > 0 {
							return true, nil, errors.New("checkpoint unavailable")
						}

						return false, nil, nil
					},
				)
			}

			err := service.Copy(t.Context(), session, 2, true, "")
			switch kind {
			case "capacity", "cleanup failure", "checkpoint failure":
				want := map[string]string{"capacity": "ran out of space", "cleanup failure": "cannot confirm release", "checkpoint failure": "checkpoint unavailable"}[kind]
				if err == nil || !strings.Contains(err.Error(), want) || len(engine.requests) > 1 {
					t.Fatalf("got %v in %d attempts", err, len(engine.requests))
				}

				if kind == "checkpoint failure" && len(engine.requests) != 0 {
					t.Fatal("engine started without checkpoint")
				}
			default:
				if err != nil || len(engine.requests) != 2 {
					t.Fatalf("got %v in %d attempts", err, len(engine.requests))
				}

				if strings.Contains(
					out.String(),
					"exceeded --copy-timeout",
				) != (kind == "timeout") {
					t.Fatal(out.String())
				}
			}
		})
	}
}

func TestCrossReservationFollowsConsumerWhileWaitingForBinding(t *testing.T) {
	service, session, _ := transferFixture(t)

	var output logBuffer
	service.WithRuntime(
		crosscluster.Runtime{
			Writer:         &output,
			Logger:         slog.New(slog.NewTextHandler(&output, nil)),
			StreamToolLogs: true,
		},
	)
	client := testutil.MustType[*fake.Clientset](t, service.DestinationClientForTest())

	sc, err := client.StorageV1().StorageClasses().Get(t.Context(), "fast", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	sc.VolumeBindingMode = new(storagev1.VolumeBindingWaitForFirstConsumer)
	if _, err = client.StorageV1().
		StorageClasses().
		Update(t.Context(), sc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	client.PrependReactor("create", "pods", func(a kt.Action) (bool, runtime.Object, error) {
		p, err := testutil.ActionObject[*corev1.Pod](a)
		if err != nil {
			return true, nil, err
		}

		p.UID = "reserve"
		p.Status.Phase = corev1.PodRunning

		return false, nil, nil
	})
	client.PrependReactor(
		"get",
		"persistentvolumeclaims",
		func(a kt.Action) (bool, runtime.Object, error) {
			// Delay binding after scheduling until the consumer's output is visible.
			if strings.Contains(output.String(), "data-copy binding") &&
				!strings.Contains(output.String(), "fake logs") {
				p, err := client.Tracker().
					Get(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), a.GetNamespace(), "data-copy")
				if err != nil {
					return true, nil, err
				}

				pending := testutil.MustType[*corev1.PersistentVolumeClaim](t, p).DeepCopy()
				pending.Status.Phase = corev1.ClaimPending

				return true, pending, nil
			}

			return false, nil, nil
		},
	)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if err := service.Copy(ctx, session, 1, true, ""); err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"data-copy binding", "scheduling", "[tool destination/app/", "reserve] fake logs"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("missing %q in %s", want, output.String())
		}
	}
}

func TestCrossResumeDoesNotStartBesideLegacyDestinationWriter(t *testing.T) {
	service, session, engine := transferFixture(t)
	// A legacy session could be interrupted before it saved any attempt count.
	volume := &session.Spec.Volumes[0]
	volume.Destination.PVC.UID = "destination-pvc"
	volume.Destination.PV = crosscluster.ClusterResourceRef{
		ClusterID:  session.Spec.DestinationCluster.ID,
		APIVersion: "v1",
		Kind:       "PersistentVolume",
		Name:       "pv-copy",
		UID:        "destination-pv",
	}
	session.Status.Volumes[0].Reservation.PVC = volume.Destination.PVC
	session.Status.Volumes[0].Reservation.PV = volume.Destination.PV

	session.Status.Phase = crosscluster.PhaseTransferring
	if err := service.SaveForTest(t.Context(), session, false); err != nil {
		t.Fatal(err)
	}

	var output logBuffer
	service.WithRuntime(crosscluster.Runtime{Logger: slog.New(slog.NewTextHandler(&output, nil))})
	client := service.DestinationClientForTest()

	_, err := client.AppsV1().Deployments("app").Create(t.Context(), &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "old-sshd",
			Namespace: "app",
			Labels:    map[string]string{kube.AppComponentLabel: "sshd"},
		},
		Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					kube.AppComponentLabel: "sshd",
					kube.AppInstanceLabel:  "pv-migrate-old-local-dest",
				},
			},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{
					{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "data-copy",
							},
						},
					},
				},
			},
		}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	finished := make(chan error, 1)
	go func() { finished <- service.Copy(ctx, session, 2, true, "") }()

	deadline := time.After(4 * time.Second)
	for !strings.Contains(output.String(), "old-sshd") {
		select {
		case err := <-finished:
			t.Fatalf("did not wait for the old writer: %v", err)
		case <-deadline:
			t.Fatal("no writer diagnostic", output.String())
		case <-time.After(time.Millisecond):
		}
	}

	cancel()

	if err := <-finished; err == nil || len(engine.requests) != 0 {
		t.Fatalf("err=%v requests=%d", err, len(engine.requests))
	}
}
