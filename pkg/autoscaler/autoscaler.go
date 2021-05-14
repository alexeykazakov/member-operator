package autoscaler

import (
	"context"
	"errors"
	"math"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	toolchainv1alpha1 "github.com/codeready-toolchain/api/pkg/apis/toolchain/v1alpha1"
	"github.com/codeready-toolchain/member-operator/pkg/controller/memberstatus"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	bufferSizeNodeSizeRatio = 0.8 // The buffer size is 80% of allocatable memory of a worker node

	priorityClassName = "autoscaling-buffer"
	bufferAppName     = "autoscaling-buffer"
)

func EnsureBuffer(cl client.Client, namespace string) error {
	if err := ensurePriorityClass(cl); err != nil {
		return err
	}

	if err := ensureBufferDeployment(cl, namespace); err != nil {
		return err
	}
	return nil
}

func ensurePriorityClass(cl client.Client) error {
	pc := &schedulingv1.PriorityClass{}
	if err := cl.Get(context.TODO(), types.NamespacedName{Name: priorityClassName}, pc); err != nil {
		if k8serrors.IsNotFound(err) {
			return createPriorityClass(cl)
		}
		return err
	}
	updated := patchPriorityClassObj(pc)
	if updated {
		for i := 0; i < 10; i++ { // Try 10 times in case of conflict before giving up
			if err := cl.Update(context.TODO(), pc); err != nil {
				if k8serrors.IsConflict(err) {
					// Re-load and re-try
					pc = &schedulingv1.PriorityClass{}
					if err := cl.Get(context.TODO(), types.NamespacedName{Name: priorityClassName}, pc); err != nil {
						return err
					}
					continue
				}
				return err
			}
			// Updated. Can exit now.
			return nil
		}
	}

	return nil
}

func createPriorityClass(cl client.Client) error {
	pc := &schedulingv1.PriorityClass{}
	patchPriorityClassObj(pc)
	return cl.Create(context.TODO(), pc)
}

func patchPriorityClassObj(pc *schedulingv1.PriorityClass) bool {
	updated := patchLabels(&pc.ObjectMeta, toolchainv1alpha1.ProviderLabelKey, toolchainv1alpha1.ProviderLabelValue)
	if pc.Value != -100 {
		pc.Value = -100
		updated = true
	}
	if pc.GlobalDefault {
		pc.GlobalDefault = false
		updated = true
	}
	description := "This priority class is to be used by the autoscaling buffer pod only"
	if pc.Description != description {
		pc.Description = description
		updated = true
	}
	return updated
}

func ensureBufferDeployment(cl client.Client, namespace string) error {
	bufferSizeGi, err := bufferSizeGi(cl)
	if err != nil {
		return err
	}

	dt := &appsv1.Deployment{}
	if err := cl.Get(context.TODO(), types.NamespacedName{Name: bufferAppName, Namespace: namespace}, dt); err != nil {
		if k8serrors.IsNotFound(err) {
			return createBufferDeployment(cl)
		}
		return err
	}
	updated := patchBufferDeployment(dt)
	if updated {
		for i := 0; i < 10; i++ { // Try 10 times in case of conflict before giving up
			if err := cl.Update(context.TODO(), pc); err != nil {
				if k8serrors.IsConflict(err) {
					// Re-load and re-try
					pc = &schedulingv1.PriorityClass{}
					if err := cl.Get(context.TODO(), types.NamespacedName{Name: priorityClassName}, pc); err != nil {
						return err
					}
					continue
				}
				return err
			}
			// Updated. Can exit now.
			return nil
		}
	}

	return nil
}

func createBufferDeployment(cl client.Client) error {
	pc := &schedulingv1.PriorityClass{}
	patchPriorityClassObj(pc)
	return cl.Create(context.TODO(), pc)
}

func patchLabels(meta *v1.ObjectMeta, key, value string) bool {
	if meta.Labels == nil {
		meta.Labels = map[string]string{}
	}
	if currentValue, found := meta.Labels[key]; !found || currentValue != value {
		meta.Labels[key] = value
		return true
	}
	return false
}

//kind: Deployment
//apiVersion: apps/v1
//metadata:
//  name: autoscaling-buffer
//  namespace: ${NAMESPACE}
//  labels:
//    app: autoscaling-buffer
//spec:
//  replicas: 1
//  selector:
//    matchLabels:
//      app: autoscaling-buffer
//  template:
//    metadata:
//      labels:
//        app: autoscaling-buffer
//    spec:
//      priorityClassName: autoscaling-buffer
//      terminationGracePeriodSeconds: 0
//      containers:
//      - name: autoscaling-buffer
//        image: gcr.io/google_containers/pause-amd64:3.0
//        resources:
//          requests:
//            memory: ${MEMORY}
//          limits:
//            memory: ${MEMORY}

func patchBufferDeployment(dt *appsv1.Deployment) bool {
	updated := patchLabels(&dt.ObjectMeta, toolchainv1alpha1.ProviderLabelKey, toolchainv1alpha1.ProviderLabelValue)
	updated = patchLabels(&dt.ObjectMeta, "app", bufferAppName) || updated

	replicas := dt.Spec.Replicas
	one := int32(1)
	if replicas == nil || *replicas != one {
		replicas = &one
		updated = true
	}
	if dt.Spec.Selector == nil || dt.Spec.Selector.MatchLabels == nil {
		if app, found := dt.Spec.Selector.MatchLabels["app"]; !found || app != bufferAppName || len(dt.Spec.Selector.MatchLabels) > 1 {
			dt.Spec.Selector = &v1.LabelSelector{MatchLabels: map[string]string{"app": bufferAppName}}
			updated = true
		}
	}
	return updated
}

func bufferSizeGi(cl client.Client) (int64, error) {
	nodes := &corev1.NodeList{}
	if err := cl.List(context.TODO(), nodes); err != nil {
		return 0, err
	}
	for _, node := range nodes.Items {
		if worker(node) {
			if memoryCapacity, found := node.Status.Allocatable["memory"]; found {
				allocatableGi := memoryCapacity.ScaledValue(resource.Giga)
				bufferSizeGi := int64(math.Round(bufferSizeNodeSizeRatio * float64(allocatableGi)))
				return bufferSizeGi, nil
			}
		}
	}
	return 0, errors.New("unable to obtain allocatable memory of a worker node")
}

func worker(node corev1.Node) bool {
	if _, isInfra := node.Labels[memberstatus.LabelNodeRoleInfra]; isInfra {
		return false
	}
	if _, isWorker := node.Labels[memberstatus.LabelNodeRoleWorker]; isWorker {
		return true
	}
	return false
}
