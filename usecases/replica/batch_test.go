package replica

import (
	"context"
	"sort"
	"strconv"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

func objectEx(id strfmt.UUID, lastTime int64, shard, node string) *storobj.Object {
	return &storobj.Object{
		Object: models.Object{
			ID:                 id,
			LastUpdateTimeUnix: lastTime,
		},
		BelongsToShard: shard,
		BelongsToNode:  node,
	}
}

func TestBatchInput(t *testing.T) {
	var (
		N    = 9
		ids  = make([]strfmt.UUID, N)
		data = make([]*storobj.Object, N)
	)
	for i := 0; i < N; i++ {
		uuid := strfmt.UUID(strconv.Itoa(i))
		ids[i] = uuid
		data[i] = objectEx(uuid, 1, "S1", "N1")
	}
	parts := cluster(createBatch(data))
	assert.Len(t, parts, 1)
	assert.Equal(t, parts[0], batchPart{
		Shard: "S1",
		Node:  "N1",
		Data:  data,
		Index: []int{0, 1, 2, 3, 4, 5, 6, 7, 8},
	})
	assert.Equal(t, parts[0].ObjectIDs(), ids)

	data[0].BelongsToShard = "S2"
	data[0].BelongsToNode = "N2"
	data[2].BelongsToShard = "S2"
	data[2].BelongsToNode = "N2"
	data[3].BelongsToShard = "S2"
	data[4].BelongsToNode = "N2"
	data[5].BelongsToShard = "S2"
	data[5].BelongsToNode = "N2"

	parts = cluster(createBatch(data))
	sort.Slice(parts, func(i, j int) bool { return len(parts[i].Index) < len(parts[j].Index) })
	assert.Len(t, parts, 2)
	assert.ElementsMatch(t, parts[0].ObjectIDs(), []strfmt.UUID{ids[0], ids[2], ids[3], ids[5]})
	assert.Equal(t, parts[0].Shard, "S2")
	assert.Equal(t, parts[0].Node, "N2")

	assert.ElementsMatch(t, parts[1].ObjectIDs(), []strfmt.UUID{ids[1], ids[4], ids[6], ids[7], ids[8]})
	assert.Equal(t, parts[1].Shard, "S1")
	assert.Equal(t, parts[1].Node, "N1")
}

func TestFinderCheckConsistencyALLForOneShard(t *testing.T) {
	var (
		ids    = []strfmt.UUID{"0", "1", "2", "3", "4", "5"}
		cls    = "C1"
		shards = []string{"S1"}
		nodes  = []string{"A", "B", "C"}
		ctx    = context.Background()
		// xs      = make([]*storobj.Object, len(ids))
		// digestR = make([]RepairResponse, len(ids))
	)

	t.Run("ExceptOne", func(t *testing.T) {
		var (
			shard       = shards[0]
			f           = newFakeFactory("C1", shard, nodes)
			finder      = f.newFinder("A")
			xs, digestR = genInputs("A", shard, ids)
		)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR, errAny)

		err := finder.CheckConsistency(ctx, All, xs)
		want := make([]*storobj.Object, len(ids))
		for i, x := range xs {
			cp := *x
			cp.IsConsistent = false
			want[i] = &cp
		}

		assert.Nil(t, err)
		assert.ElementsMatch(t, want, xs)
		// f.assertLogContains(t, "replica", nodes[2])
		f.assertLogErrorContains(t, errRead.Error())
	})

	t.Run("Success", func(t *testing.T) {
		var (
			shard       = shards[0]
			f           = newFakeFactory("C1", shard, nodes)
			finder      = f.newFinder("A")
			xs, digestR = genInputs("A", shard, ids)
		)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR, nil)

		want := make([]*storobj.Object, len(ids))
		for i, x := range xs {
			cp := *x
			cp.IsConsistent = true
			want[i] = &cp
		}

		err := finder.CheckConsistency(ctx, All, xs)
		assert.Nil(t, err)
		assert.ElementsMatch(t, want, xs)
	})
}

func TestFinderCheckConsistencyALLForTwoShard(t *testing.T) {
	var (
		ids    = []strfmt.UUID{"0", "1", "2", "3", "4", "5"}
		cls    = "C1"
		shards = []string{"S1", "S2"}
		nodes  = []string{"A", "B", "C"}
		ctx    = context.Background()
	)

	t.Run("Success", func(t *testing.T) {
		var (
			f             = newFakeFactory("C1", shards[0], nodes)
			finder        = f.newFinder("A")
			idSet1        = ids[:3]
			idSet2        = ids[3:6]
			xs1, digestR1 = genInputs("A", shards[0], idSet1)
			xs2, digestR2 = genInputs("B", shards[1], idSet2)
		)
		xs := make([]*storobj.Object, 0, len(xs1)+len(xs2))
		for i := 0; i < 3; i++ {
			xs = append(xs, xs1[i])
			xs = append(xs, xs2[i])
		}
		// first shard
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shards[0], idSet1).Return(digestR1, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shards[0], idSet1).Return(digestR1, nil)

		// second shard
		f.AddShard(shards[1], nodes)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shards[1], idSet2).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shards[1], idSet2).Return(digestR2, nil)

		want := make([]*storobj.Object, len(ids))
		for i, x := range xs {
			cp := *x
			cp.IsConsistent = true
			want[i] = &cp
		}

		err := finder.CheckConsistency(ctx, All, xs)
		assert.Nil(t, err)
		assert.ElementsMatch(t, want, xs)
	})
}

func genInputs(node, shard string, ids []strfmt.UUID) ([]*storobj.Object, []RepairResponse) {
	xs := make([]*storobj.Object, len(ids))
	digestR := make([]RepairResponse, len(ids))
	for i, id := range ids {
		xs[i] = &storobj.Object{
			Object: models.Object{
				ID:                 id,
				LastUpdateTimeUnix: int64(i),
			},
			BelongsToShard: shard,
			BelongsToNode:  node,
		}
		digestR[i] = RepairResponse{ID: ids[i].String(), UpdateTime: int64(i)}
	}
	return xs, digestR
}
