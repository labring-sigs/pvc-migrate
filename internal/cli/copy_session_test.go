package cli

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

type copyLookupStore struct {
	lookup func(domain.SessionType) (*domain.Session, error)
	calls  []domain.SessionType
}

func (*copyLookupStore) StorageBackend() string { return kube.SessionBackendCRD }

func (*copyLookupStore) Create(context.Context, *domain.Session) error {
	return errors.New("unused")
}

func (*copyLookupStore) Get(context.Context, string, string) (*domain.Session, error) {
	return nil, errors.New("unused")
}

func (s *copyLookupStore) GetByType(
	_ context.Context,
	_ string,
	_ string,
	sessionType domain.SessionType,
) (*domain.Session, error) {
	s.calls = append(s.calls, sessionType)
	return s.lookup(sessionType)
}

func (*copyLookupStore) Update(context.Context, *domain.Session) error {
	return errors.New("unused")
}

func (*copyLookupStore) List(context.Context, string) ([]*domain.Session, error) {
	return nil, errors.New("unused")
}

func (*copyLookupStore) Delete(context.Context, *domain.Session) error {
	return errors.New("unused")
}

func TestGetCopySessionFallsBackToReservation(t *testing.T) {
	reserved := domain.NewSession(
		"reserved",
		domain.NewSessionSpec(
			domain.OperationReserve,
			domain.SessionCommon{SessionNamespace: "tenant"},
			false,
			domain.SessionWorkflowOptions{},
		),
		time.Time{},
	)

	tests := []struct {
		name      string
		namespace string
		id        string
	}{
		{name: "namespaced reservation", namespace: "tenant", id: "reserved"},
		{name: "cluster reservation", namespace: "pvc-migrate-system", id: "cluster-reserved"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &copyLookupStore{
				lookup: func(sessionType domain.SessionType) (*domain.Session, error) {
					if sessionType == domain.SessionTypeCopy {
						return nil, domain.NewError(
							domain.ErrorValidation,
							"get session",
							"session "+tt.namespace+"/"+tt.id+" does not exist",
						)
					}

					return reserved, nil
				},
			}

			got, err := getCopySession(context.Background(), store, tt.namespace, tt.id)
			if err != nil {
				t.Fatal(err)
			}

			if got != reserved {
				t.Fatalf("session=%p, want reserved session %p", got, reserved)
			}

			want := []domain.SessionType{domain.SessionTypeCopy, domain.SessionTypeReserve}
			if !reflect.DeepEqual(store.calls, want) {
				t.Fatalf("lookup calls=%v, want %v", store.calls, want)
			}
		})
	}
}

func TestGetCopySessionDoesNotFallbackOnOtherErrors(t *testing.T) {
	wantErr := errors.New("permission denied")
	store := &copyLookupStore{
		lookup: func(sessionType domain.SessionType) (*domain.Session, error) {
			if sessionType != domain.SessionTypeCopy {
				t.Fatalf("unexpected fallback lookup for %s", sessionType)
			}

			return nil, domain.WrapError(
				domain.ErrorKubernetes,
				"get session",
				"read Copy",
				wantErr,
			)
		},
	}

	_, err := getCopySession(context.Background(), store, "tenant", "copy")
	if !errors.Is(err, wantErr) {
		t.Fatalf("error=%v, want wrapped %v", err, wantErr)
	}

	want := []domain.SessionType{domain.SessionTypeCopy}
	if !reflect.DeepEqual(store.calls, want) {
		t.Fatalf("lookup calls=%v, want %v", store.calls, want)
	}
}

func TestGetCopySessionReturnsCopyWithoutFallback(t *testing.T) {
	copySession := domain.NewSession(
		"copy",
		domain.NewSessionSpec(
			domain.OperationCopy,
			domain.SessionCommon{SessionNamespace: "tenant"},
			false,
			domain.SessionWorkflowOptions{},
		),
		time.Time{},
	)
	store := &copyLookupStore{
		lookup: func(sessionType domain.SessionType) (*domain.Session, error) {
			if sessionType != domain.SessionTypeCopy {
				t.Fatalf("unexpected fallback lookup for %s", sessionType)
			}

			return copySession, nil
		},
	}

	got, err := getCopySession(context.Background(), store, "tenant", copySession.ID)
	if err != nil {
		t.Fatal(err)
	}

	if got != copySession {
		t.Fatalf("session=%p, want copy session %p", got, copySession)
	}

	want := []domain.SessionType{domain.SessionTypeCopy}
	if !reflect.DeepEqual(store.calls, want) {
		t.Fatalf("lookup calls=%v, want %v", store.calls, want)
	}
}
