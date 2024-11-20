package applier

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"strings"

	"github.com/operator-framework/operator-controller/internal/authentication"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/postrender"
	"helm.sh/helm/v3/pkg/release"
	"helm.sh/helm/v3/pkg/storage/driver"
	corev1 "k8s.io/api/core/v1"
	errv1 "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	helmclient "github.com/operator-framework/helm-operator-plugins/pkg/client"

	ocv1alpha1 "github.com/operator-framework/operator-controller/api/v1alpha1"
	"github.com/operator-framework/operator-controller/internal/rukpak/util"
)

type Helmer struct {
	ActionClientGetter helmclient.ActionClientGetter
	TokenGetter        *authentication.TokenGetter
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
	chartFromFS, err := loadChartFromFS(contentFS)
	if err != nil {
		return nil, "", err
	}
	values := chartutil.Values{}

	values, err = processConfig(ctx, h.TokenGetter, ext, values)
	if err != nil {
		return nil, "", fmt.Errorf("Unable to process config: %v", err)
	}

	ac, err := h.ActionClientGetter.ActionClientFor(ctx, ext)
	if err != nil {
		return nil, "", err
	}

	post := &postrenderer{
		labels: objectLabels,
	}

	rel, _, state, err := h.getReleaseState(ac, ext, chartFromFS, values, post)
	if err != nil {
		return nil, "", err
	}

	switch state {
	case StateNeedsInstall:
		rel, err = ac.Install(ext.GetName(), ext.Spec.Install.Namespace, chartFromFS, values, func(install *action.Install) error {
			install.CreateNamespace = false
			install.Labels = storageLabels
			return nil
		}, helmclient.AppendInstallPostRenderer(post))
		if err != nil {
			return nil, state, err
		}
	case StateNeedsUpgrade:
		rel, err = ac.Upgrade(ext.GetName(), ext.Spec.Install.Namespace, chartFromFS, values, func(upgrade *action.Upgrade) error {
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

// createClientWithToken creates a new client that uses the specified token.
func createClientWithToken(token string) (*kubernetes.Clientset, error) {

	// Get the default config
	cfg, err := rest.InClusterConfig()

	// Remove existing credentials
	anonCfg := rest.AnonymousClientConfig(cfg)

	// Create a custom rest config using the token
	cfgCopy := rest.CopyConfig(anonCfg)
	cfgCopy.BearerToken = token

	clientSet, err := kubernetes.NewForConfig(cfgCopy)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client with token: %w", err)
	}

	return clientSet, nil
}

// Looks for the ConfigSources by name in the install namespace and gathers values
func processConfig(ctx context.Context, tokenGetter *authentication.TokenGetter, ext *ocv1alpha1.ClusterExtension, values chartutil.Values) (chartutil.Values, error) {

	// Create or get a token for the provided service account
	token, err := tokenGetter.Get(ctx, types.NamespacedName{Namespace: ext.Spec.Install.Namespace, Name: ext.Spec.Install.ServiceAccount.Name})
	if err != nil {
		log.Fatalf("failed to get token for service account %s/%s: %v", ext.Spec.Install.Namespace, ext.Spec.Install.ServiceAccount.Name, err)
	}

	// Create a client with the provided service account token
	authedClientSet, err := createClientWithToken(token)
	if err != nil {
		log.Fatalf("failed to create client with token: %v", err)
	}

	var configMapList []corev1.ConfigMap
	var secretList []corev1.Secret
	var textConfigList []string

	if ext.Spec.Install.ConfigSources != nil {
		configSources := *ext.Spec.Install.ConfigSources
		// Process config map names
		if configSources.ConfigMapNames != nil && len(configSources.ConfigMapNames) > 0 {
			for _, configMapName := range configSources.ConfigMapNames {
				configMap := &corev1.ConfigMap{}
				configMap, err := authedClientSet.CoreV1().ConfigMaps(ext.Spec.Install.Namespace).Get(ctx, configMapName, metav1.GetOptions{})
				if err != nil && !errv1.IsNotFound(err) {
					return values, fmt.Errorf("failed to retrieve ConfigMap: %v", err)
				}
				configMapList = append(configMapList, *configMap)
			}
		}
		// Process secrets
		if configSources.SecretNames != nil && len(configSources.SecretNames) > 0 {
			for _, secretName := range configSources.SecretNames {
				secret := &corev1.Secret{}
				secret, err := authedClientSet.CoreV1().Secrets(ext.Spec.Install.Namespace).Get(ctx, secretName, metav1.GetOptions{})
				if err != nil && !errv1.IsNotFound(err) {
					return values, fmt.Errorf("failed to retrieve ConfigMap: %v", err)
				}
				secretList = append(secretList, *secret)
			}
		}
		// Process plain text configs
		if configSources.TextConfigs != nil && len(configSources.TextConfigs) > 0 {
			for _, textConfig := range configSources.TextConfigs {
				textConfigList = append(textConfigList, textConfig)
			}
		}
	}

	// If the ConfigSources have been found, build values from the data
	if len(configMapList) > 0 || len(secretList) > 0 || len(textConfigList) > 0 {
		// combine all configMaps
		for _, configMap := range configMapList {
			valuesYaml, found := configMap.Data["values.yaml"]
			if found {
				userValuesMap, err := chartutil.ReadValues([]byte(valuesYaml))
				if err != nil {
					return values, fmt.Errorf("failed to parse values.yaml from ConfigMap %v: %v", configMap.Name, err)
				}
				// combine all values files
				values = chartutil.MergeTables(values, userValuesMap)
			}
		}

		//combine all secrets
		for _, secret := range secretList {
			valuesYaml, found := secret.Data["values.yaml"]
			if found {
				userValuesMap, err := chartutil.ReadValues([]byte(valuesYaml))
				if err != nil {
					return nil, fmt.Errorf("failed to parse values.yaml from secret %v: %v", secret.Name, err)
				}
				// combine all values files
				values = chartutil.MergeTables(values, userValuesMap)
			}
		}

		//combine all secrets
		for _, textConfig := range textConfigList {
			if len(textConfig) > 0 {
				userValuesMap, err := chartutil.ReadValues([]byte(textConfig))
				if err != nil {
					return nil, fmt.Errorf("failed to parse values from textConfig: %v", err)
				}
				// combine all values files
				values = chartutil.MergeTables(values, userValuesMap)
			}
		}
	}

	return values, nil
}
