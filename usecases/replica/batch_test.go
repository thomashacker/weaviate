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

func TestFinderCheckConsistencyWithConsistencyLevelALL(t *testing.T) {
	var (
		ids    = []strfmt.UUID{"0", "1", "2", "3", "4", "5"}
		cls    = "C1"
		shards = []string{"S1"}
		nodes  = []string{"A", "B", "C"}
		ctx    = context.Background()
		// xs      = make([]*storobj.Object, len(ids))
		// digestR = make([]RepairResponse, len(ids))
	)

	gen := func(ids []strfmt.UUID) ([]*storobj.Object, []RepairResponse) {
		xs := make([]*storobj.Object, len(ids))
		digestR := make([]RepairResponse, len(ids))
		for i, id := range ids {
			xs[i] = &storobj.Object{
				Object: models.Object{
					ID:                 id,
					LastUpdateTimeUnix: int64(i),
				},
				BelongsToShard: shards[0],
				BelongsToNode:  "A",
			}
			digestR[i] = RepairResponse{ID: ids[i].String(), UpdateTime: int64(i)}
		}
		return xs, digestR
	}

	t.Run("All", func(t *testing.T) {
		var (
			shard       = shards[0]
			f           = newFakeFactory("C1", shard, nodes)
			finder      = f.newFinder("A")
			xs, digestR = gen(ids)
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


	t.Run("AllButOne", func(t *testing.T) {
		var (
			shard       = shards[0]
			f           = newFakeFactory("C1", shard, nodes)
			finder      = f.newFinder("A")
			xs, digestR = gen(ids)
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

		assert.ElementsMatch(t, want, xs)
		//assert.ErrorIs(t, err, errRead)
		assert.ErrorContains(t, err, msgCLevel)
		f.assertLogContains(t, "replica", nodes[2])
		f.assertLogErrorContains(t, errAny.Error())
	})
}
