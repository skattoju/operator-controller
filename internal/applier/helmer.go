package applier

import (
	"context"
	"errors"
	"fmt"
	"gopkg.in/yaml.v2"
	"helm.sh/helm/v3/pkg/chart/loader"
	"io"
	"io/fs"
	corev1 "k8s.io/api/core/v1"
	errv1 "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"strings"

	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/postrender"
	"helm.sh/helm/v3/pkg/release"
	"helm.sh/helm/v3/pkg/storage/driver"
	"sigs.k8s.io/controller-runtime/pkg/client"

	helmclient "github.com/operator-framework/helm-operator-plugins/pkg/client"

	ocv1alpha1 "github.com/operator-framework/operator-controller/api/v1alpha1"
	"github.com/operator-framework/operator-controller/internal/rukpak/util"
)

type Helmer struct {
	ActionClientGetter helmclient.ActionClientGetter
	ConfigMapName      string
	Namespace          string
	Client             client.Client
}

func loadChartFromFS(fsys fs.FS) (*chart.Chart, error) {
	var files []*loader.BufferedFile

	// Walk through the file system and gather the chart files
	err := fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Ignore directories
		if d.IsDir() {
			return nil
		}

		// Open the file from fs.FS
		file, err := fsys.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		// Read the file content
		content, err := io.ReadAll(file)
		if err != nil {
			return err
		}

		// Create a BufferedFile with the content
		files = append(files, &loader.BufferedFile{Name: path, Data: content})

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error walking file system: %v", err)
	}

	// Load the chart from the in-memory files
	chart, err := loader.LoadFiles(files)
	if err != nil {
		return nil, fmt.Errorf("failed to load chart: %v", err)
	}

	return chart, nil
}

func (h *Helmer) Apply(ctx context.Context, contentFS fs.FS, ext *ocv1alpha1.ClusterExtension, objectLabels map[string]string, storageLabels map[string]string) ([]client.Object, string, error) {
	chrt, err := loadChartFromFS(contentFS)
	if err != nil {
		return nil, "", err
	}
	values := chartutil.Values{}

	// Look for the ConfigMap with the specified name and namespace
	// here for testing I'm using a pre-configured test configMap in test namespace
	// TODO: find a way to pass this config map through the ClusterExtension specs
	var userValuesMap map[string]interface{}
	configMap := &corev1.ConfigMap{}
	err = h.Client.Get(ctx, types.NamespacedName{Name: h.ConfigMapName, Namespace: h.Namespace}, configMap)
	if err != nil && !errv1.IsNotFound(err) {
		return nil, "", fmt.Errorf("failed to retrieve ConfigMap: %v", err)
	}

	// If the ConfigMap is found, parse the values.yaml from the data
	if err == nil {
		valuesYaml, found := configMap.Data["values.yaml"]
		if found {
			userValuesMap, err = parseValuesYaml([]byte(valuesYaml))
			if err != nil {
				return nil, "", fmt.Errorf("failed to parse values.yaml from ConfigMap: %v", err)
			}
			values = chartutil.CoalesceTables(values, userValuesMap)
		}
	}

	ac, err := h.ActionClientGetter.ActionClientFor(ctx, ext)
	if err != nil {
		return nil, "", err
	}

	post := &postrenderer{
		labels: objectLabels,
	}

	rel, _, state, err := h.getReleaseState(ac, ext, chrt, values, post)
	if err != nil {
		return nil, "", err
	}

	switch state {
	case StateNeedsInstall:
		rel, err = ac.Install(ext.GetName(), ext.Spec.Install.Namespace, chrt, values, func(install *action.Install) error {
			install.CreateNamespace = false
			install.Labels = storageLabels
			return nil
		}, helmclient.AppendInstallPostRenderer(post))
		if err != nil {
			return nil, state, err
		}
	case StateNeedsUpgrade:
		rel, err = ac.Upgrade(ext.GetName(), ext.Spec.Install.Namespace, chrt, values, func(upgrade *action.Upgrade) error {
			upgrade.MaxHistory = maxHelmReleaseHistory
			upgrade.Labels = storageLabels
			return nil
		}, helmclient.AppendUpgradePostRenderer(post))
		if err != nil {
			return nil, state, err
		}
	case StateUnchanged:
		if err := ac.Reconcile(rel); err != nil {
			return nil, state, err
		}
	default:
		return nil, state, fmt.Errorf("unexpected release state %q", state)
	}

	relObjects, err := util.ManifestObjects(strings.NewReader(rel.Manifest), fmt.Sprintf("%s-release-manifest", rel.Name))
	if err != nil {
		return nil, state, err
	}

	return relObjects, state, nil
}

func (h *Helmer) getReleaseState(cl helmclient.ActionInterface, ext *ocv1alpha1.ClusterExtension, chrt *chart.Chart, values chartutil.Values, post postrender.PostRenderer) (*release.Release, *release.Release, string, error) {
	currentRelease, err := cl.Get(ext.GetName())
	if err != nil && !errors.Is(err, driver.ErrReleaseNotFound) {
		return nil, nil, StateError, err
	}
	if errors.Is(err, driver.ErrReleaseNotFound) {
		return nil, nil, StateNeedsInstall, nil
	}

	if errors.Is(err, driver.ErrReleaseNotFound) {
		desiredRelease, err := cl.Install(ext.GetName(), ext.Spec.Install.Namespace, chrt, values, func(i *action.Install) error {
			i.DryRun = true
			i.DryRunOption = "server"
			return nil
		}, helmclient.AppendInstallPostRenderer(post))
		if err != nil {
			return nil, nil, StateError, err
		}
		return nil, desiredRelease, StateNeedsInstall, nil
	}
	desiredRelease, err := cl.Upgrade(ext.GetName(), ext.Spec.Install.Namespace, chrt, values, func(upgrade *action.Upgrade) error {
		upgrade.MaxHistory = maxHelmReleaseHistory
		upgrade.DryRun = true
		upgrade.DryRunOption = "server"
		return nil
	}, helmclient.AppendUpgradePostRenderer(post))
	if err != nil {
		return currentRelease, nil, StateError, err
	}
	relState := StateUnchanged
	if desiredRelease.Manifest != currentRelease.Manifest ||
		currentRelease.Info.Status == release.StatusFailed ||
		currentRelease.Info.Status == release.StatusSuperseded {
		relState = StateNeedsUpgrade
	}
	return currentRelease, desiredRelease, relState, nil
}

func parseValuesYaml(yamlContent []byte) (map[string]interface{}, error) {
	var values map[string]interface{}
	err := yaml.Unmarshal(yamlContent, &values)
	if err != nil {
		return nil, fmt.Errorf("failed to parse values.yaml: %v", err)
	}
	return values, nil
}
