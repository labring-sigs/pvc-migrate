package kube

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func TestRepositoryCredentialWriteReportsLostOwnershipAndRetainsRecovery(t *testing.T) {
	for _, failure := range []string{"cancellation", "lease loss"} {
		t.Run(failure, func(t *testing.T) {
			client, repository, owner := repositoryStoreFixture()

			store := NewConfigMapRepositoryStore(client)
			if err := store.Create(t.Context(), repository, owner); err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			lock := &workflowLeaseTestLock{}
			ctx = WithLeaseFence(ctx, lock)

			failureErr := errors.New("lease replaced during credential creation")
			if failure == "cancellation" {
				failureErr = context.Canceled
			}

			client.PrependReactor(
				"create",
				"secrets",
				func(ktesting.Action) (bool, runtime.Object, error) {
					if failure == "cancellation" {
						cancel()
					} else {
						lock.fenceErr = failureErr
					}

					return false, nil, nil
				},
			)

			err := store.CreateCredentials(ctx, repository.Namespace, owner, map[string][]byte{
				BackupAccessKeyDataKey: []byte("access"), BackupSecretKeyDataKey: []byte("secret"),
			})
			if !errors.Is(err, failureErr) {
				t.Fatalf("credential write did not report ownership loss: %v", err)
			}

			secret, err := client.CoreV1().Secrets(repository.Namespace).
				Get(t.Context(), BackupCredentialsSecretName(owner.Name), metav1.GetOptions{})
			if err != nil || string(secret.Data[BackupAccessKeyDataKey]) != "access" {
				t.Fatalf("credential recovery was discarded: %v", err)
			}

			if _, err := store.Load(
				t.Context(),
				crclient.ObjectKeyFromObject(repository),
			); err != nil {
				t.Fatalf("repository recovery was discarded: %v", err)
			}
		})
	}
}

func TestRepositoryOwnedCleanupHandlesPartialCreation(t *testing.T) {
	for _, withRepository := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "secret only", true: "repository and secret"}[withRepository],
			func(t *testing.T) {
				client, repository, owner := repositoryStoreFixture()

				store := NewConfigMapRepositoryStore(client)
				if withRepository {
					if err := store.Create(t.Context(), repository, owner); err != nil {
						t.Fatal(err)
					}
				}

				if err := store.CreateCredentials(
					t.Context(),
					repository.Namespace,
					owner,
					map[string][]byte{
						BackupAccessKeyDataKey: []byte("access"),
						BackupSecretKeyDataKey: []byte("secret"),
					},
				); err != nil {
					t.Fatal(err)
				}

				secret, err := client.CoreV1().
					Secrets(repository.Namespace).
					Get(t.Context(), BackupCredentialsSecretName(owner.Name), metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}

				secret.UID, secret.ResourceVersion = "secret", "1"
				if _, err := client.CoreV1().
					Secrets(secret.Namespace).
					Update(t.Context(), secret, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}

				userSecret := &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: repository.Namespace,
						Name:      "user-credentials",
					},
				}
				if _, err := client.CoreV1().
					Secrets(userSecret.Namespace).
					Create(t.Context(), userSecret, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}

				key := crclient.ObjectKeyFromObject(repository)
				if err := store.ValidateOwnedCleanup(t.Context(), key, owner); err != nil {
					t.Fatal(err)
				}

				if _, err := client.CoreV1().
					Secrets(secret.Namespace).
					Get(t.Context(), secret.Name, metav1.GetOptions{}); err != nil {
					t.Fatalf("validation deleted credentials: %v", err)
				}

				for range 2 {
					if err := store.CleanupOwned(t.Context(), key, owner); err != nil {
						t.Fatal(err)
					}
				}

				if _, err := client.CoreV1().
					Secrets(secret.Namespace).
					Get(t.Context(), secret.Name, metav1.GetOptions{}); !apierrors.IsNotFound(
					err,
				) {
					t.Fatalf("owned secret remains: %v", err)
				}

				if _, err := client.CoreV1().
					Secrets(userSecret.Namespace).
					Get(t.Context(), userSecret.Name, metav1.GetOptions{}); err != nil {
					t.Fatalf("user credentials removed: %v", err)
				}

				if _, err := store.Load(t.Context(), key); !apierrors.IsNotFound(err) {
					t.Fatalf("repository remains: %v", err)
				}
			},
		)
	}
}

func TestRepositoryCleanupRetainsRepositoryOnCredentialFailure(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "delete failure", true: "foreign owner"}[foreign],
			func(t *testing.T) {
				client, repository, owner := repositoryStoreFixture()

				store := NewConfigMapRepositoryStore(client)
				if err := store.Create(t.Context(), repository, owner); err != nil {
					t.Fatal(err)
				}

				if err := store.CreateCredentials(
					t.Context(),
					repository.Namespace,
					owner,
					map[string][]byte{
						BackupAccessKeyDataKey: []byte("access"),
						BackupSecretKeyDataKey: []byte("secret"),
					},
				); err != nil {
					t.Fatal(err)
				}

				secret, err := client.CoreV1().
					Secrets(repository.Namespace).
					Get(t.Context(), BackupCredentialsSecretName(owner.Name), metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}

				secret.UID, secret.ResourceVersion = "secret", "1"
				if foreign {
					secret.Annotations[repositoryOwnerUID] = "another-workflow"
				}

				if _, err := client.CoreV1().
					Secrets(secret.Namespace).
					Update(t.Context(), secret, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}

				client.PrependReactor(
					"delete",
					"secrets",
					func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, errors.New("delete failed") },
				)
				client.ClearActions()

				key := crclient.ObjectKeyFromObject(repository)
				if err := store.CleanupOwned(t.Context(), key, owner); err == nil {
					t.Fatal("unsafe cleanup succeeded")
				}

				for _, action := range client.Actions() {
					if action.GetVerb() == "delete" &&
						(foreign || action.GetResource().Resource == "configmaps") {
						t.Fatalf("unexpected deletion: %v", action)
					}
				}

				if _, err := store.Load(t.Context(), key); err != nil {
					t.Fatalf("repository removed before credentials: %v", err)
				}
			},
		)
	}
}
