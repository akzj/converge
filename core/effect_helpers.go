package core

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/akzj/converge/pkg/model"
)

func newEffectID() (EffectID, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return EffectID("eff-" + hex.EncodeToString(value[:])), nil
}

func newReferenceID(configID model.ConfigID, planID model.PlanID, generation model.Generation, effectKey string) ReferenceID {
	return ReferenceID(fmt.Sprintf("%s/%s/%d/%s", configID.Name, string(planID), uint64(generation), effectKey))
}
