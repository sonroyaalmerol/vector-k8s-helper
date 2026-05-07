// Package writer persists generated Vector configuration to a Kubernetes ConfigMap.
package writer

import (
	"context"
	"fmt"
	"log/slog"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// Writer updates a ConfigMap with the generated Vector configuration.
type Writer struct {
	client       kubernetes.Interface
	namespace    string
	configMapKey string
	logger       *slog.Logger
}

// NewWriter creates a Writer targeting the specified namespace and ConfigMap key.
func NewWriter(client kubernetes.Interface, namespace, configMapKey string, logger *slog.Logger) *Writer {
	return &Writer{
		client:       client,
		namespace:    namespace,
		configMapKey: configMapKey,
		logger:       logger,
	}
}

// Upsert creates or updates the target ConfigMap with the provided content.
func (w *Writer) Upsert(ctx context.Context, name string, content []byte) error {
	if w.namespace == "" {
		return fmt.Errorf("namespace must be set for ConfigMap operations")
	}

	cmClient := w.client.CoreV1().ConfigMaps(w.namespace)

	_, err := cmClient.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		// Assume not found; create a new ConfigMap.
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: w.namespace,
			},
			Data: map[string]string{
				w.configMapKey: string(content),
			},
		}
		_, err = cmClient.Create(ctx, cm, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create configmap %s/%s: %w", w.namespace, name, err)
		}
		w.logger.Info("created configmap", "namespace", w.namespace, "name", name)
		return nil
	}

	// Update existing ConfigMap using strategic merge patch for efficiency.
	patch := fmt.Sprintf(
		`{"data":{"%s":%q}}`,
		w.configMapKey,
		string(content),
	)
	_, err = cmClient.Patch(ctx, name, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch configmap %s/%s: %w", w.namespace, name, err)
	}
	w.logger.Info("updated configmap", "namespace", w.namespace, "name", name, "size", len(content))
	return nil
}
