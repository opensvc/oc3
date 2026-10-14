package serverhandlers

import (
	"database/sql"
	"testing"

	"github.com/opensvc/oc3/cdb"
)

func TestCheckValidation(t *testing.T) {
	if err := validChkType(""); err == nil {
		t.Error("an empty check type is accepted")
	}
	if err := validChkType("verylongtype"); err == nil {
		t.Error("a check type longer than the column is accepted")
	}
	if err := validChkType("fs_u"); err != nil {
		t.Errorf("fs_u is refused: %s", err)
	}
	if err := validPattern("chk_inst", "("); err == nil {
		t.Error("an invalid pattern is accepted")
	}
	if err := validPattern("chk_inst", ""); err != nil {
		t.Errorf("the empty pattern, every instance, is refused: %s", err)
	}
	low := sql.NullInt64{Int64: 10, Valid: true}
	high := sql.NullInt64{Int64: 5, Valid: true}
	if err := validThresholds(low, high); err == nil {
		t.Error("a low threshold above the high one is accepted")
	}
	if err := validThresholds(low, sql.NullInt64{}); err != nil {
		t.Errorf("a low threshold alone is refused: %s", err)
	}
	if err := validContextual(cdb.CheckContextual{Type: "fs_u", Low: 0, High: 9}); err == nil {
		t.Error("contextual thresholds without an instance pattern are accepted")
	}
}

func TestCheckDefaultChanges(t *testing.T) {
	before := cdb.CheckDefault{Type: "fs_u", Inst: "", High: sql.NullInt64{Int64: 98, Valid: true}}
	after := before
	after.High = sql.NullInt64{}
	after.Prio = 1
	if got, want := checkDefaultChanges(before, after), "chk_high: 98 => none, chk_prio: 0 => 1"; got != want {
		t.Errorf("changes %q, want %q", got, want)
	}
}
