package core

import (
	"github.com/cockroachdb/errors"

	"github.com/akzj/converge/pkg/model"
)

// validateConfigDependencies rejects cycles before publishing desired state.
// Missing upstream configs are allowed: they keep the config Converging until
// submitted, but cannot form a known cycle yet.
func (r *Reconciler) validateConfigDependencies(candidate model.DesiredState) error {
	r.mu.RLock()
	dependencies := make(map[string][]string, len(r.configs)+1)
	for name, managed := range r.configs {
		dependencies[name] = append([]string(nil), managed.DependsOnConfigs...)
	}
	r.mu.RUnlock()
	dependencies[candidate.ConfigID.Name] = append([]string(nil), candidate.DependsOn...)

	const (
		unvisited = iota
		visiting
		visited
	)
	colors := make(map[string]int, len(dependencies))
	var visit func(string) error
	visit = func(name string) error {
		switch colors[name] {
		case visiting:
			return errors.Errorf("configuration dependency cycle contains %q", name)
		case visited:
			return nil
		}
		colors[name] = visiting
		for _, dependency := range dependencies[name] {
			if _, known := dependencies[dependency]; !known {
				continue
			}
			if err := visit(dependency); err != nil {
				return err
			}
		}
		colors[name] = visited
		return nil
	}
	for name := range dependencies {
		if err := visit(name); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reconciler) transitiveDependents(upstream string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := make(map[string]bool)
	queue := []string{upstream}
	var result []string
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for name, managed := range r.configs {
			if seen[name] || name == upstream {
				continue
			}
			for _, dependency := range managed.DependsOnConfigs {
				if dependency == current {
					seen[name] = true
					result = append(result, name)
					queue = append(queue, name)
					break
				}
			}
		}
	}
	return result
}
