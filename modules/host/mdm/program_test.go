package mdm

import (
	"context"
	"testing"

	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/types"
	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/fastrand"
)

// TestNewEmptyProgram runs a program without instructions.
func TestNewEmptyProgram(t *testing.T) {
	// Create MDM
	mdm := New(newTestHost())
	// Shouldn't be able to execute empty program.
	pt := newTestPriceTable()
	_, _, err := mdm.ExecuteProgram(context.Background(), pt, modules.NewProgram(), modules.MDMInitCost(pt, 0, 0), types.ZeroCurrency, newTestStorageObligation(true))
	if !errors.Contains(err, ErrEmptyProgram) {
		t.Fatal("expected ErrEmptyProgram", err)
	}
}

// TestNewProgramLowInitBudget runs a program that doesn't even have enough funds to init the MDM.
func TestNewProgramLowInitBudget(t *testing.T) {
	// Create MDM
	mdm := New(newTestHost())
	program, _, _, _ := newHasSectorProgram(crypto.Hash{}, newTestPriceTable())
	// Execute the program.
	pt := newTestPriceTable()
	_, _, err := mdm.ExecuteProgram(context.Background(), pt, program, types.ZeroCurrency, types.ZeroCurrency, newTestStorageObligation(true))
	if !errors.Contains(err, modules.ErrMDMInsufficientBudget) {
		t.Fatal("missing error")
	}
}

// TestNewProgramLowBudget runs a program with instructions with insufficient
// funds.
func TestNewProgramLowBudget(t *testing.T) {
	// Create MDM
	mdm := New(newTestHost())
	// Create instruction.
	pt := newTestPriceTable()
	program, _, values, err := newReadSectorProgram(modules.SectorSize, 0, crypto.Hash{}, true, pt)
	if err != nil {
		t.Fatal(err)
	}
	// Execute the program with enough money to init the mdm but not enough
	// money to execute the first instruction.
	cost := modules.MDMInitCost(pt, program.DataLen, 1)
	finalize, outputs, err := mdm.ExecuteProgram(context.Background(), pt, program, cost, values.Collateral, newTestStorageObligation(true))
	if err != nil {
		t.Fatal(err)
	}
	// The first output should contain an error.
	numOutputs := 0
	numInsufficientBudgetErrs := 0
	for output := range outputs {
		if err := output.Error; errors.Contains(err, modules.ErrMDMInsufficientBudget) {
			numInsufficientBudgetErrs++
		} else if err != nil {
			t.Fatal(err)
		}
		numOutputs++
	}
	if numOutputs != 1 {
		t.Fatalf("numOutputs was %v but should be %v", numOutputs, 1)
	}
	if numInsufficientBudgetErrs != 1 {
		t.Fatalf("numInsufficientBudgetErrs was %v but should be %v", numInsufficientBudgetErrs, 1)
	}
	// Finalize should be nil for readonly programs.
	if finalize != nil {
		t.Fatal("finalize should be 'nil' for readonly programs")
	}
}

// TestNewProgramLowCollateralBudget runs a program with instructions with insufficient
// collateral budget.
func TestNewProgramLowCollateralBudget(t *testing.T) {
	// Create MDM
	mdm := New(newTestHost())
	// Create instruction.
	pt := newTestPriceTable()
	program, _, values, err := newAppendProgram(fastrand.Bytes(int(modules.SectorSize)), false, pt)
	if err != nil {
		t.Fatal(err)
	}
	// Execute the program with no collateral budget.
	so := newTestStorageObligation(true)
	finalize, outputs, err := mdm.ExecuteProgram(context.Background(), pt, program, values.ExecutionCost, types.ZeroCurrency, so)
	if err != nil {
		t.Fatal(err)
	}
	// The first output should contain an error.
	numOutputs := 0
	numInsufficientBudgetErrs := 0
	for output := range outputs {
		if err := output.Error; errors.Contains(err, modules.ErrMDMInsufficientCollateralBudget) {
			numInsufficientBudgetErrs++
		} else if err != nil {
			t.Fatalf("%v: using budget %v", err, values.HumanString())
		}
		numOutputs++
	}
	if numOutputs != 1 {
		t.Fatalf("numOutputs was %v but should be %v", numOutputs, 1)
	}
	if numInsufficientBudgetErrs != 1 {
		t.Fatalf("numInsufficientBudgetErrs was %v but should be %v", numInsufficientBudgetErrs, 1)
	}
	// Try to finalize program. Should fail.
	if err := finalize(so); err == nil {
		t.Fatal("shouldn't be able to finalize program")
	}
}
