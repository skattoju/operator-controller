package source

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/containers/image/v5/pkg/blobinfocache/none"
	"github.com/containers/image/v5/types"
	"github.com/opencontainers/go-digest"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type TarGZ struct {
	BaseCachePath string
}

func (i *TarGZ) Unpack(ctx context.Context, bundle *BundleSource) (*Result, error) {
	l := log.FromContext(ctx)

	if bundle.Image == nil {
		return nil, reconcile.TerminalError(fmt.Errorf("error parsing bundle, bundle %s has a nil image source", bundle.Name))
	}

	// Download the .tgz file
	resp, err := http.Get(bundle.Image.Ref)
	if err != nil {
		return nil, reconcile.TerminalError(fmt.Errorf("error downloading bundle '%s': %v", bundle.Name, err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, reconcile.TerminalError(fmt.Errorf("error downloading bundle '%s': got status code '%d'", bundle.Name, resp.StatusCode))
	}

	// Open a gzip reader
	gzReader, err := gzip.NewReader(resp.Body)
	if err != nil {
		return nil, reconcile.TerminalError(fmt.Errorf("error unpacking bundle '%s': %v", bundle.Name, err))
	}
	defer gzReader.Close()

	unpackDir := path.Join(i.BaseCachePath, bundle.Name)
	err = os.MkdirAll(unpackDir, 0700)
	if err != nil {
		return nil, fmt.Errorf("error creating temporary directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(unpackDir); err != nil {
			l.Error(err, "error removing temporary OCI layout directory")
		}
	}()

	// Open a tar reader
	tarReader := tar.NewReader(gzReader)

	_hack := ""

	// Extract tar contents
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return nil, reconcile.TerminalError(fmt.Errorf("error unpaking bundle '%s': %v", bundle.Name, err))
		}

		// Construct the target file path
		targetPath := filepath.Join(unpackDir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			// Create directory
			if err := os.MkdirAll(targetPath, os.FileMode(header.Mode)); err != nil {
				return nil, reconcile.TerminalError(fmt.Errorf("error unpacking bundle '%s': %v", bundle.Name, err))
			}
		case tar.TypeReg:
			// Ensure the directory for the file exists
			if err := os.MkdirAll(filepath.Dir(targetPath), os.FileMode(0700)); err != nil {
				return nil, reconcile.TerminalError(fmt.Errorf("error unpacking bundle '%s': %v", bundle.Name, err))
			}
			if _hack == "" {
				_hack = filepath.Join(unpackDir, filepath.Dir(targetPath))
			}

			// Create a file
			outFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY, os.FileMode(header.Mode))
			if err != nil {
				return nil, reconcile.TerminalError(fmt.Errorf("error unpacking bundle '%s': %v", bundle.Name, err))
			}
			if _, err := io.Copy(outFile, tarReader); err != nil {
				outFile.Close()
				return nil, reconcile.TerminalError(fmt.Errorf("error unpacking bundle '%s': %v", bundle.Name, err))
			}
			outFile.Close()
		default:
			// Handle other types of files if necessary (e.g., symlinks, etc.)
			l.V(2).Info("Skipping unsupported file type in tar: %s\n", header.Name)
		}
	}

	return successHelmUnpackResult(bundle.Name, _hack, bundle.Image.Ref), nil
}

func successHelmUnpackResult(bundleName, unpackPath string, chartgz string) *Result {
	return &Result{
		Bundle:         os.DirFS(unpackPath),
		ResolvedSource: &BundleSource{Type: SourceTypeImage, Name: bundleName, Image: &ImageSource{Ref: chartgz}},
		State:          StateUnpacked,
		Message:        fmt.Sprintf("unpacked %q successfully", chartgz),
	}
}

func (i *TarGZ) Cleanup(_ context.Context, bundle *BundleSource) error {
	return deleteRecursive(i.bundlePath(bundle.Name))
}

func (i *TarGZ) bundlePath(bundleName string) string {
	return filepath.Join(i.BaseCachePath, bundleName)
}

func (i *TarGZ) unpackPath(bundleName string, digest digest.Digest) string {
	return filepath.Join(i.bundlePath(bundleName), digest.String())
}

func (i *TarGZ) unpackImage(ctx context.Context, unpackPath string, imageReference types.ImageReference, sourceContext *types.SystemContext) error {
	img, err := imageReference.NewImage(ctx, sourceContext)
	if err != nil {
		return fmt.Errorf("error reading image: %w", err)
	}
	defer func() {
		if err := img.Close(); err != nil {
			panic(err)
		}
	}()

	layoutSrc, err := imageReference.NewImageSource(ctx, sourceContext)
	if err != nil {
		return fmt.Errorf("error creating image source: %w", err)
	}

	if err := os.MkdirAll(unpackPath, 0700); err != nil {
		return fmt.Errorf("error creating unpack directory: %w", err)
	}
	l := log.FromContext(ctx)
	l.Info("unpacking image", "path", unpackPath)
	for i, layerInfo := range img.LayerInfos() {
		if err := func() error {
			layerReader, _, err := layoutSrc.GetBlob(ctx, layerInfo, none.NoCache)
			if err != nil {
				return fmt.Errorf("error getting blob for layer[%d]: %w", i, err)
			}
			defer layerReader.Close()

			if err := applyLayer(ctx, unpackPath, layerReader); err != nil {
				return fmt.Errorf("error applying layer[%d]: %w", i, err)
			}
			l.Info("applied layer", "layer", i)
			return nil
		}(); err != nil {
			return errors.Join(err, deleteRecursive(unpackPath))
		}
	}
	if err := setReadOnlyRecursive(unpackPath); err != nil {
		return fmt.Errorf("error making unpack directory read-only: %w", err)
	}
	return nil
}

func (i *TarGZ) deleteOtherImages(bundleName string, digestToKeep digest.Digest) error {
	bundlePath := i.bundlePath(bundleName)
	imgDirs, err := os.ReadDir(bundlePath)
	if err != nil {
		return fmt.Errorf("error reading image directories: %w", err)
	}
	for _, imgDir := range imgDirs {
		if imgDir.Name() == digestToKeep.String() {
			continue
		}
		imgDirPath := filepath.Join(bundlePath, imgDir.Name())
		if err := deleteRecursive(imgDirPath); err != nil {
			return fmt.Errorf("error removing image directory: %w", err)
		}
	}
	return nil
}
