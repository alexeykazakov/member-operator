package nstemplateset

import (
	toolchainv1alpha1 "github.com/codeready-toolchain/api/api/v1alpha1"
	"github.com/codeready-toolchain/toolchain-common/pkg/template"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/strings/slices"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
)

// shouldCreate checks if the object has a feature toggle annotation. If it does then check if the corresponding
// feature is referenced in the NSTemplateSet feature annotation. Returns true if yes. It means this feature
// should be enabled and the object should be created. It also returns true if the object doesn't have a feature annotation at all
// which means it's a regular object, and it's not managed by any feature toggle and should be always created.
// Otherwise, returns false.
func shouldCreate(toCreate runtimeclient.Object, nsTmplSet *toolchainv1alpha1.NSTemplateSet) bool {
	return shouldCreateForFeatures(toCreate, nsTmplSet.GetAnnotations()[toolchainv1alpha1.FeatureToggleNameAnnotationKey])
}

// shouldCreateObjectForFeatures returns true if the object is expected to be created according to the list of enabed features
// also returns true if the object does not represent any features
func shouldCreateForFeatures(toCreate runtimeclient.Object, features string) bool {
	feature, found := toCreate.GetAnnotations()[toolchainv1alpha1.FeatureToggleNameAnnotationKey]
	if !found {
		return true // This object is a regular object and not managed by a feature toggle. Always create it.
	}
	return slices.Contains(splitCommaSeparatedList(features), feature)
}

// splitCommaSeparatedList acts exactly the same as strings.Split(s, ",") but returns an empty slice for empty strings.
// To be used when, for example, we want to get an empty slice for empty comma separated list:
// strings.Split("", ",") returns [""] while splitCommaSeparatedList("") returns []
func splitCommaSeparatedList(s string) []string {
	if len(s) == 0 {
		return []string{}
	}
	return strings.Split(s, ",")
}

// retainFeatureEnabled creates a filter which retains objects which should be present according to
// the enabled features specified in the NSTemplateSet
func retainFeatureEnabled(nsTmplSet *toolchainv1alpha1.NSTemplateSet) template.FilterFunc {
	return retainEnabledForFeatures(nsTmplSet.GetAnnotations()[toolchainv1alpha1.FeatureToggleNameAnnotationKey])
}

// retainFeatureEnabledForFeatures creates a filter which retains objects which should be present according to
// the enabled features
func retainEnabledForFeatures(features string) template.FilterFunc {
	return func(obj runtime.RawExtension) bool {
		clientObj, ok := obj.Object.(runtimeclient.Object)
		if !ok {
			return false
		}
		return shouldCreateForFeatures(clientObj, features)
	}
}

// TODO move to API
var LastAppliedFeaturesAnnotationKey = "toolchain.dev.openshift.com/last-applied-features"
