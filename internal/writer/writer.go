package writer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
)

type Writer struct {
	client       kubernetes.Interface
	namespace    string
	configMapKey string
	logger       *slog.Logger
}

func NewWriter(client kubernetes.Interface, namespace, configMapKey string, logger *slog.Logger) *Writer {
	return &Writer{
		client:       client,
		namespace:    namespace,
		configMapKey: configMapKey,
		logger:       logger,
	}
}

func (w *Writer) Upsert(ctx context.Context, name string, content []byte) error {
	if w.namespace == "" {
		return fmt.Errorf("namespace must be set for ConfigMap operations")
	}

	cmClient := w.client.CoreV1().ConfigMaps(w.namespace)

	patch, err := buildPatch(w.configMapKey, content)
	if err != nil {
		return fmt.Errorf("failed to encode configmap patch: %w", err)
	}

	const maxAttempts = 3
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = w.apply(ctx, cmClient, name, content, patch)
		if err == nil {
			return nil
		}
		lastErr = err
		if !shouldRetry(err) || attempt == maxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * 200 * time.Millisecond):
		}
	}
	return fmt.Errorf("failed to write configmap %s/%s: %w", w.namespace, name, lastErr)
}

func (w *Writer) apply(ctx context.Context, cmClient corev1client.ConfigMapInterface, name string, content []byte, patch []byte) error {
	_, err := cmClient.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
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
			return err
		}
		w.logger.Info("created configmap", "namespace", w.namespace, "name", name)
		return nil
	}

	_, err = cmClient.Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return err
	}
	w.logger.Info("updated configmap", "namespace", w.namespace, "name", name, "size", len(content))
	return nil
}

func shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	if apierrors.IsConflict(err) {
		return true
	}
	return errors.Is(err, context.DeadlineExceeded) || isTransientNet(err)
}

func buildPatch(key string, content []byte) ([]byte, error) {
	body := map[string]any{
		"data": map[string]string{
			key: string(content),
		},
	}
	return json.Marshal(body)
}

func isTransientNet(err error) bool {
	var t interface{ Timeout() bool }
	if errors.As(err, &t) {
		return t.Timeout()
	}
	return false
}
