package controller

import (
	"bytes"
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

type podCommandRequest struct {
	Namespace string
	Pod       string
	Container string
	Command   []string
}

type podCommandResult struct {
	Stdout string
	Stderr string
}

type podCommandExecutor interface {
	Execute(context.Context, podCommandRequest) (podCommandResult, error)
}

type podCommandExecutorFunc func(context.Context, podCommandRequest) (podCommandResult, error)

func (f podCommandExecutorFunc) Execute(ctx context.Context, request podCommandRequest) (podCommandResult, error) {
	return f(ctx, request)
}

type kubernetesPodCommandExecutor struct {
	client kubernetes.Interface
	config *rest.Config
}

func (e kubernetesPodCommandExecutor) Execute(ctx context.Context, request podCommandRequest) (podCommandResult, error) {
	if e.client == nil || e.config == nil {
		return podCommandResult{}, fmt.Errorf("kubernetes pod exec client is not configured")
	}
	req := e.client.CoreV1().RESTClient().Post().
		Namespace(request.Namespace).
		Resource("pods").
		Name(request.Pod).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: request.Container,
			Command:   request.Command,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(e.config, "POST", req.URL())
	if err != nil {
		return podCommandResult{}, err
	}
	var stdout, stderr bytes.Buffer
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr})
	return podCommandResult{Stdout: stdout.String(), Stderr: stderr.String()}, err
}
