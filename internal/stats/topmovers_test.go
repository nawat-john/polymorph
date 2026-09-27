package stats

import "testing"

func TestTopMoversOrdering(t *testing.T) {
	movers := []Mover{
		{AssetID: "a", Chg5m: 1.0},
		{AssetID: "b", Chg5m: -9.0}, // largest by magnitude
		{AssetID: "c", Chg5m: 4.0},
		{AssetID: "d", Chg5m: -2.0},
		{AssetID: "e", Chg5m: 0.0},
	}

	top := TopMovers(movers, 3)
	if len(top) != 3 {
		t.Fatalf("len(TopMovers(_, 3)) = %d, want 3", len(top))
	}
	wantOrder := []string{"b", "c", "d"} // by |chg_5m|: 9, 4, 2
	for i, id := range wantOrder {
		if top[i].AssetID != id {
			t.Errorf("top[%d].AssetID = %q, want %q (full result: %+v)", i, top[i].AssetID, id, top)
		}
	}

	// Input slice must not be mutated.
	if movers[0].AssetID != "a" || movers[1].AssetID != "b" {
		t.Errorf("TopMovers mutated its input slice: %+v", movers)
	}
}

func TestTopMoversNNotLimiting(t *testing.T) {
	movers := []Mover{{AssetID: "a", Chg5m: 1}, {AssetID: "b", Chg5m: 2}}
	if got := TopMovers(movers, 0); len(got) != 2 {
		t.Fatalf("TopMovers(_, 0) len = %d, want 2 (n<=0 means unlimited)", len(got))
	}
	if got := TopMovers(movers, 50); len(got) != 2 {
		t.Fatalf("TopMovers(_, 50) len = %d, want 2 (n larger than input)", len(got))
	}
}
