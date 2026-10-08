package kube

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// WorkflowAbortRequestedAnnotation carries a declarative abort request. The
// value is the request time. An actively reconciled workflow holds its session
// lease for whole transfer attempts, so a competing process can never win the
// lease mid-flight; requesters record the annotation and the controller, which
// interrupts its active reconcile for the request, executes the abort itself.
const WorkflowAbortRequestedAnnotation = "migrate.sealos.io/abort-requested"

// WorkflowAbortRequested reports a pending abort request on the workflow.
func WorkflowAbortRequested(object metav1.Object) bool {
	return object.GetAnnotations()[WorkflowAbortRequestedAnnotation] != ""
}

// WorkflowAbortRequestChanged admits abort-request updates to the workflow
// queues without admitting unrelated annotation churn.
func WorkflowAbortRequestChanged(previous, current metav1.Object) bool {
	return previous.GetAnnotations()[WorkflowAbortRequestedAnnotation] !=
		current.GetAnnotations()[WorkflowAbortRequestedAnnotation]
}

// AbortRequestStore persists the declarative abort request. Only the CRD
// backend implements it: session records have no controller to hand the
// request to, and their aborts already run in the requesting process.
type AbortRequestStore[T crclient.Object] interface {
	// SetAbortRequest records the request (value != "") or consumes a
	// recorded one (value == ""). Only workflow metadata is updated.
	SetAbortRequest(ctx context.Context, object T, value string) error
}
