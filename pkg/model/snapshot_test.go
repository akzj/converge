package model

import "testing"

func testDesiredSnapshot(t *testing.T, revision uint64, items ...DesiredState) DesiredSnapshot {
	t.Helper()
	for i := range items {
		items[i].Digest = DesiredSpecDigest(items[i].Spec)
	}
	digest, err := DesiredSnapshotDigest(revision, items)
	if err != nil {
		t.Fatal(err)
	}
	return DesiredSnapshot{Revision: revision, Digest: digest, Items: items}
}

func TestDesiredSnapshotDigestIsOrderIndependent(t *testing.T) {
	a := DesiredState{ConfigID: ConfigID{Name: "a"}, ProviderType: "test", Version: 1, Spec: []byte(`{"a":1}`), DependsOn: []string{"c", "b"}}
	b := DesiredState{ConfigID: ConfigID{Name: "b"}, ProviderType: "test", Version: 1, Spec: []byte(`{"b":1}`)}
	first := testDesiredSnapshot(t, 7, a, b)
	second := testDesiredSnapshot(t, 7, b, a)
	second.Items[1].DependsOn = []string{"b", "c"}
	second.Digest, _ = DesiredSnapshotDigest(second.Revision, second.Items)
	if first.Digest != second.Digest {
		t.Fatalf("digest depends on item/dependency order: %s != %s", first.Digest, second.Digest)
	}
}

func TestCausalContextDoesNotChangeDesiredIdentity(t *testing.T) {
	desired := DesiredState{ConfigID: ConfigID{Name: "a"}, ProviderType: "test", Version: 1, Spec: []byte(`{"a":1}`)}
	desired.Digest = DesiredSpecDigest(desired.Spec)
	firstIdentity, err := DesiredStateIdentityDigest(desired)
	if err != nil {
		t.Fatal(err)
	}
	firstSnapshot, err := DesiredSnapshotDigest(1, []DesiredState{desired})
	if err != nil {
		t.Fatal(err)
	}
	desired.Cause = CausalContext{TraceParent: "trace", TraceState: "state", CorrelationID: "correlation", CausationID: "cause"}
	secondIdentity, err := DesiredStateIdentityDigest(desired)
	if err != nil {
		t.Fatal(err)
	}
	secondSnapshot, err := DesiredSnapshotDigest(1, []DesiredState{desired})
	if err != nil {
		t.Fatal(err)
	}
	if firstIdentity != secondIdentity || firstSnapshot != secondSnapshot {
		t.Fatalf("causal metadata changed desired identity: identity %s/%s snapshot %s/%s", firstIdentity, secondIdentity, firstSnapshot, secondSnapshot)
	}
}

func TestValidateDesiredSnapshotRejectsInvalidGraphAndDigest(t *testing.T) {
	a := DesiredState{ConfigID: ConfigID{Name: "a"}, ProviderType: "test", Version: 1, Spec: []byte(`{}`), DependsOn: []string{"b"}}
	b := DesiredState{ConfigID: ConfigID{Name: "b"}, ProviderType: "test", Version: 1, Spec: []byte(`{}`), DependsOn: []string{"a"}}
	cyclic := testDesiredSnapshot(t, 1, a, b)
	if err := ValidateDesiredSnapshot(cyclic); err == nil {
		t.Fatal("cyclic snapshot was accepted")
	}
	valid := testDesiredSnapshot(t, 1, DesiredState{ConfigID: ConfigID{Name: "a"}, ProviderType: "test", Version: 1, Spec: []byte(`{}`)})
	valid.Digest = "sha256:wrong"
	if err := ValidateDesiredSnapshot(valid); err == nil {
		t.Fatal("invalid envelope digest was accepted")
	}
}
