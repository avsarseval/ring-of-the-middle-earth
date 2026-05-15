package internal

import (
	"encoding/json"
	"testing"
)

func makeTestEvent(t *testing.T, order OrderPayload) Event {
	t.Helper()

	payload, err := json.Marshal(order)
	if err != nil {
		t.Fatalf("order marshal edilemedi: %v", err)
	}

	return Event{
		Topic:   TopicOrdersRaw,
		Payload: payload,
	}
}

func TestValidationWrongTurn(t *testing.T) {
	setupTestWorld(t)

	order := OrderPayload{
		OrderType: OrderAssignRoute,
		PlayerID:  "light",
		UnitID:    RingBearerID,
		Turn:      2,
		PathIDs:   []string{"shire-to-bree"},
	}

	code, message := validateWrongTurn(order, RingBearerID, false)

	if code != "WRONG_TURN" {
		t.Fatalf("expected WRONG_TURN, got %s message=%s", code, message)
	}
}

func TestValidationNotYourUnit(t *testing.T) {
	setupTestWorld(t)

	order := OrderPayload{
		OrderType: OrderAssignRoute,
		PlayerID:  "shadow",
		UnitID:    RingBearerID,
		Turn:      1,
		PathIDs:   []string{"shire-to-bree"},
	}

	code, message := validateUnitOwnershipRule(order, RingBearerID, false)

	if code != "NOT_YOUR_UNIT" {
		t.Fatalf("expected NOT_YOUR_UNIT, got %s message=%s", code, message)
	}
}

func TestValidationInvalidPath(t *testing.T) {
	setupTestWorld(t)

	order := OrderPayload{
		OrderType: OrderAssignRoute,
		PlayerID:  "light",
		UnitID:    RingBearerID,
		Turn:      1,
		PathIDs:   []string{"wrong-path"},
	}

	code, message := validatePathRule(order, RingBearerID, false)

	if code != "INVALID_PATH" {
		t.Fatalf("expected INVALID_PATH, got %s message=%s", code, message)
	}
}

func TestValidationDuplicateUnitOrder(t *testing.T) {
	setupTestWorld(t)

	markUnitOrdered(1, RingBearerID)

	order := OrderPayload{
		OrderType: OrderAssignRoute,
		PlayerID:  "light",
		UnitID:    RingBearerID,
		Turn:      1,
		PathIDs:   []string{"shire-to-bree"},
	}

	code, message := validateDuplicateRule(order, RingBearerID, false)

	if code != "DUPLICATE_UNIT_ORDER" {
		t.Fatalf("expected DUPLICATE_UNIT_ORDER, got %s message=%s", code, message)
	}
}

func TestValidationUnitNotAdjacent(t *testing.T) {
	setupTestWorld(t)

	order := OrderPayload{
		OrderType: OrderBlockPath,
		PlayerID:  "light",
		UnitID:    "aragorn",
		Turn:      1,
		PathID:    "rivendell-to-moria",
	}

	code, message := validateUnitAdjacentRule(order, "aragorn", false)

	if code != "UNIT_NOT_ADJACENT" {
		t.Fatalf("expected UNIT_NOT_ADJACENT, got %s message=%s", code, message)
	}
}

func TestValidationPathBlocked(t *testing.T) {
	setupTestWorld(t)

	setPathStatus("shire-to-bree", "BLOCKED")
	UpdatePathStatus("shire-to-bree", "BLOCKED")

	order := OrderPayload{
		OrderType: OrderAssignRoute,
		PlayerID:  "light",
		UnitID:    RingBearerID,
		Turn:      1,
		PathIDs:   []string{"shire-to-bree"},
	}

	code, message := validatePathBlockedRule(order, RingBearerID, false)

	if code != "PATH_BLOCKED" {
		t.Fatalf("expected PATH_BLOCKED, got %s message=%s", code, message)
	}
}

func TestValidationInvalidTarget(t *testing.T) {
	setupTestWorld(t)

	order := OrderPayload{
		OrderType:    OrderAttackRegion,
		PlayerID:     "light",
		UnitID:       "aragorn",
		Turn:         1,
		TargetRegion: "",
	}

	code, message := validateAttackTargetRule(order, "aragorn", false)

	if code != "INVALID_TARGET" {
		t.Fatalf("expected INVALID_TARGET, got %s message=%s", code, message)
	}
}

func TestValidationAbilityOnCooldown(t *testing.T) {
	setupTestWorld(t)

	setCooldown("gandalf", 2)

	order := OrderPayload{
		OrderType: OrderMaiaAbility,
		PlayerID:  "light",
		UnitID:    "gandalf",
		Turn:      1,
		PathID:    "minas-tirith-to-osgiliath",
	}

	code, message := validateCooldownRule(order, "gandalf", false)

	if code != "ABILITY_ON_COOLDOWN" {
		t.Fatalf("expected ABILITY_ON_COOLDOWN, got %s message=%s", code, message)
	}
}