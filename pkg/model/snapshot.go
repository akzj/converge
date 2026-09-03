package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"

	"github.com/cockroachdb/errors"
)

// ValidateDesiredSnapshot validates identities, per-item digests, uniqueness,
// dependencies, and the envelope digest before durable acceptance.
func ValidateDesiredSnapshot(snapshot DesiredSnapshot) error {
	if snapshot.Revision == 0 {
		return errors.New("desired snapshot revision is zero")
	}
	seen := make(map[string]struct{}, len(snapshot.Items))
	for _, desired := range snapshot.Items {
		name := desired.ConfigID.Name
		if name == "" {
			return errors.New("desired snapshot contains an empty config ID")
		}
		if desired.ProviderType == "" {
			return errors.Errorf("desired config %q has an empty provider type", name)
		}
		if desired.Version == 0 {
			return errors.Errorf("desired config %q has a zero version", name)
		}
		if desired.Digest != DesiredSpecDigest(desired.Spec) {
			return errors.Errorf("desired config %q digest mismatch", name)
		}
		if _, exists := seen[name]; exists {
			return errors.Errorf("desired snapshot contains duplicate config %q", name)
		}
		seen[name] = struct{}{}
		seenDependencies := make(map[string]struct{}, len(desired.DependsOn))
		for _, dependency := range desired.DependsOn {
			if dependency == name {
				return errors.Errorf("desired config %q depends on itself", name)
			}
			if _, exists := seenDependencies[dependency]; exists {
				return errors.Errorf("desired config %q has duplicate dependency %q", name, dependency)
			}
			seenDependencies[dependency] = struct{}{}
		}
	}
	if err := validateSnapshotDependencies(snapshot.Items); err != nil {
		return err
	}
	expected, err := DesiredSnapshotDigest(snapshot.Revision, snapshot.Items)
	if err != nil {
		return err
	}
	if snapshot.Digest != expected {
		return errors.Errorf("desired snapshot digest mismatch: got %q, want %q", snapshot.Digest, expected)
	}
	return nil
}

func validateSnapshotDependencies(items []DesiredState) error {
	dependencies := make(map[string][]string, len(items))
	for _, item := range items {
		dependencies[item.ConfigID.Name] = item.DependsOn
	}
	colors := make(map[string]uint8, len(items))
	var visit func(string) error
	visit = func(name string) error {
		switch colors[name] {
		case 1:
			return errors.Errorf("desired snapshot dependency cycle contains %q", name)
		case 2:
			return nil
		}
		colors[name] = 1
		for _, dependency := range dependencies[name] {
			if _, exists := dependencies[dependency]; !exists {
				return errors.Errorf("desired config %q depends on missing config %q", name, dependency)
			}
			if err := visit(dependency); err != nil {
				return err
			}
		}
		colors[name] = 2
		return nil
	}
	for name := range dependencies {
		if err := visit(name); err != nil {
			return err
		}
	}
	return nil
}

// DesiredSnapshot is the complete set of configurations owned by one Edge
// Agent. Configurations omitted by a newer snapshot are deletion intents.
type DesiredSnapshot struct {
	Revision uint64         `json:"revision"`
	Digest   string         `json:"digest"`
	Items    []DesiredState `json:"items"`
}

// CloneDesiredSnapshot returns a detached copy suitable for crossing API and
// persistence boundaries.
func CloneDesiredSnapshot(snapshot DesiredSnapshot) DesiredSnapshot {
	copy := snapshot
	copy.Items = make([]DesiredState, len(snapshot.Items))
	for i := range snapshot.Items {
		copy.Items[i] = CloneDesiredState(snapshot.Items[i])
	}
	return copy
}

// DesiredSnapshotDigest hashes a canonical ordering of the complete snapshot.
// The revision is included so the digest identifies the exact wire revision.
func DesiredSnapshotDigest(revision uint64, items []DesiredState) (string, error) {
	canonical := make([]DesiredState, len(items))
	for i := range items {
		canonical[i] = CloneDesiredState(items[i])
		slices.Sort(canonical[i].DependsOn)
	}
	slices.SortFunc(canonical, func(a, b DesiredState) int {
		return cmpString(a.ConfigID.Name, b.ConfigID.Name)
	})
	payload, err := json.Marshal(struct {
		Revision uint64         `json:"revision"`
		Items    []DesiredState `json:"items"`
	}{Revision: revision, Items: canonical})
	if err != nil {
		return "", errors.Wrap(err, "encode desired snapshot digest")
	}
	digest := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

// DesiredStateIdentityDigest covers every field that participates in revision
// identity, unlike DesiredState.Digest which intentionally covers Spec only.
func DesiredStateIdentityDigest(desired DesiredState) (string, error) {
	canonical := CloneDesiredState(desired)
	slices.Sort(canonical.DependsOn)
	payload, err := json.Marshal(canonical)
	if err != nil {
		return "", errors.Wrap(err, "encode desired identity digest")
	}
	digest := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func cmpString(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
