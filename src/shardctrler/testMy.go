package shardctrler

import (
	"fmt"
	"testing"
)

func TestMinimalAgain1(t *testing.T) {
	const nservers = 3
	cfg := make_config(t, nservers, false)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	fmt.Printf("Test: minimal movement again ...\n")

	ck.Join(map[int][]string{1: []string{"x", "y", "z"}})

	ck.Join(map[int][]string{2: []string{"a", "b", "c"}})

	c1 := ck.Query(-1)

	ck.Join(map[int][]string{3: []string{"d", "e", "f"}})

	c2 := ck.Query(-1)

	// fmt.Printf("after join(3):\n%v\n%v\n", c1.Shards, c2.Shards)

	// any shard that wasn't moved to gid 3 should
	// stay where it was.
	for i := 0; i < NShards; i++ {
		if c2.Shards[i] != 3 {
			if c1.Shards[i] != c2.Shards[i] {
				t.Fatalf("shard %v moved from gid %v to gid %v, but shouldn't have\n", i, c1.Shards[i], c2.Shards[i])
			}
		}
	}

	// a maximum of NShards/3 + 1 shards should move
	changed := 0
	for i := 0; i < NShards; i++ {
		if c1.Shards[i] != c2.Shards[i] {
			changed += 1
		}
	}
	if changed > (NShards/3)+1 {
		t.Fatalf("too many shards (%v) moved after join\n", changed)
	}

	// now gid 1 leaves.
	ck.Leave([]int{1})
	c3 := ck.Query(-1)
	// fmt.Printf("after leave(1):\n%v\n%v\n", c2.Shards, c3.Shards)

	// any shard that wasn't in gid 1 should
	// stay where it was.
	for i := 0; i < NShards; i++ {
		if c2.Shards[i] != 1 {
			if c2.Shards[i] != c3.Shards[i] {
				t.Fatalf("shard %v moved from gid %v to gid %v, but shouldn't have\n", i, c2.Shards[i], c3.Shards[i])
			}
		}
	}

	// a maximum of NShards/3 + 1 shards should move
	changed = 0
	for i := 0; i < NShards; i++ {
		if c2.Shards[i] != c3.Shards[i] {
			changed += 1
		}
	}
	if changed > (NShards/3)+1 {
		t.Fatalf("too many shards (%v) moved after leave\n", changed)
	}

	fmt.Printf("  ... Passed\n")
}

//func main() {
//	TestMinimalAgain1()
//}
