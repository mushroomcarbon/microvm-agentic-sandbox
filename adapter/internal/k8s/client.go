package k8s

import (
	"context"
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type Client struct {
	cs        *kubernetes.Clientset
	namespace string
}

// New returns a k8s client using either in-cluster config or KUBECONFIG.
func New(namespace string) (*Client, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			kubeconfig = "/etc/rancher/k3s/k3s.yaml"
		}
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("load kubeconfig: %w", err)
		}
	}

	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build clientset: %w", err)
	}
	return &Client{cs: cs, namespace: namespace}, nil
}

// CreateSandboxPod creates a Kata pod running the guest-agent image.
func (c *Client) CreateSandboxPod(ctx context.Context, sandboxID, image string) (*corev1.Pod, error) {
	pullPolicy := corev1.PullNever
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-" + sandboxID,
			Namespace: c.namespace,
			Labels: map[string]string{
				"sandbox-oss/managed":    "true",
				"sandbox-oss/sandbox-id": sandboxID,
			},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName: ptr("kata-clh"),
			Containers: []corev1.Container{{
				Name:            "agent",
				Image:           image,
				ImagePullPolicy: pullPolicy,
				Ports: []corev1.ContainerPort{{
					Name:          "agent",
					ContainerPort: 50051,
				}},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1"),
						corev1.ResourceMemory: resource.MustParse("512Mi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("2"),
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
				},
			}},
		},
	}
	return c.cs.CoreV1().Pods(c.namespace).Create(ctx, pod, metav1.CreateOptions{})
}

// DeleteSandboxPod deletes a sandbox pod by sandbox ID.
func (c *Client) DeleteSandboxPod(ctx context.Context, sandboxID string) error {
	return c.cs.CoreV1().Pods(c.namespace).Delete(ctx, "sandbox-"+sandboxID, metav1.DeleteOptions{})
}

// GetSandboxPod fetches the pod for a given sandbox ID.
func (c *Client) GetSandboxPod(ctx context.Context, sandboxID string) (*corev1.Pod, error) {
	return c.cs.CoreV1().Pods(c.namespace).Get(ctx, "sandbox-"+sandboxID, metav1.GetOptions{})
}

func ptr[T any](v T) *T { return &v }
