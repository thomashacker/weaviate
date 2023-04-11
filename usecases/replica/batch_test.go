package replica

import (
	"context"
	"sort"
	"strconv"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
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

func TestFinderCheckConsistencyALL(t *testing.T) {
	var (
		ids    = []strfmt.UUID{"0", "1", "2", "3", "4", "5"}
		cls    = "C1"
		shards = []string{"S1", "S2", "S3"}
		nodes  = []string{"A", "B", "C"}
		ctx    = context.Background()
	)

	t.Run("ExceptOne", func(t *testing.T) {
		var (
			shard       = shards[0]
			f           = newFakeFactory("C1", shard, nodes)
			finder      = f.newFinder("A")
			xs, digestR = genInputs("A", shard, 1, ids)
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

	t.Run("OneShard", func(t *testing.T) {
		var (
			shard       = shards[0]
			f           = newFakeFactory("C1", shard, nodes)
			finder      = f.newFinder("A")
			xs, digestR = genInputs("A", shard, 2, ids)
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

	t.Run("TwoShards", func(t *testing.T) {
		var (
			f             = newFakeFactory("C1", shards[0], nodes)
			finder        = f.newFinder("A")
			idSet1        = ids[:3]
			idSet2        = ids[3:6]
			xs1, digestR1 = genInputs("A", shards[0], 1, idSet1)
			xs2, digestR2 = genInputs("B", shards[1], 2, idSet2)
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

	t.Run("ThreeShard", func(t *testing.T) {
		var (
			f             = newFakeFactory("C1", shards[0], nodes)
			finder        = f.newFinder("A")
			ids1          = ids[:2]
			ids2          = ids[2:4]
			ids3          = ids[4:]
			xs1, digestR1 = genInputs("A", shards[0], 1, ids1)
			xs2, digestR2 = genInputs("B", shards[1], 2, ids2)
			xs3, digestR3 = genInputs("C", shards[2], 3, ids3)
		)
		xs := make([]*storobj.Object, 0, len(xs1)+len(xs2))
		for i := 0; i < 2; i++ {
			xs = append(xs, xs1[i])
			xs = append(xs, xs2[i])
			xs = append(xs, xs3[i])
		}
		// first shard
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shards[0], ids1).Return(digestR1, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shards[0], ids1).Return(digestR1, nil)

		// second shard
		f.AddShard(shards[1], nodes)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shards[1], ids2).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shards[1], ids2).Return(digestR2, nil)

		// third shard
		f.AddShard(shards[2], nodes)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shards[2], ids3).Return(digestR3, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shards[2], ids3).Return(digestR3, nil)

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

	t.Run("TwoShardSingleNode", func(t *testing.T) {
		var (
			f             = newFakeFactory("C1", shards[0], nodes)
			finder        = f.newFinder("A")
			ids1          = ids[:2]
			ids2          = ids[2:4]
			ids3          = ids[4:]
			xs1, digestR1 = genInputs("A", shards[0], 1, ids1)
			xs2, digestR2 = genInputs("B", shards[1], 1, ids2)
			xs3, digestR3 = genInputs("A", shards[2], 2, ids3)
		)
		xs := make([]*storobj.Object, 0, len(xs1)+len(xs2))
		for i := 0; i < 2; i++ {
			xs = append(xs, xs1[i])
			xs = append(xs, xs2[i])
			xs = append(xs, xs3[i])
		}
		// first shard
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shards[0], ids1).Return(digestR1, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shards[0], ids1).Return(digestR1, nil)

		// second shard
		f.AddShard(shards[1], nodes)
		f.RClient.On("DigestObjects", anyVal, nodes[0], cls, shards[1], ids2).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shards[1], ids2).Return(digestR2, nil)

		// third shard
		f.AddShard(shards[2], nodes)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shards[2], ids3).Return(digestR3, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shards[2], ids3).Return(digestR3, nil)

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

func TestRepairerCheckConsistencyAll(t *testing.T) {
	var (
		ids   = []strfmt.UUID{"01", "02", "03"}
		cls   = "C1"
		shard = "S1"
		nodes = []string{"A", "B", "C"}
		ctx   = context.Background()
	)
	t.Run("DirectRead", func(t *testing.T) {
		var (
			f       = newFakeFactory("C1", shard, nodes)
			finder  = f.newFinder("A")
			directR = []*storobj.Object{
				objectEx(ids[0], 4, shard, "A"),
				objectEx(ids[1], 5, shard, "A"),
				objectEx(ids[2], 6, shard, "A"),
			}
			digestR2 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 4},
				{ID: ids[1].String(), UpdateTime: 2},
				{ID: ids[2].String(), UpdateTime: 3},
			}
			digestR3 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 5},
				{ID: ids[2].String(), UpdateTime: 3},
			}
			want = setObjectsConsistency(directR, true)
		)

		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, anyVal).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, anyVal).Return(digestR3, nil)

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(digestR2, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					LatestObject:    &directR[1].Object,
					StaleUpdateTime: 2,
				},
				{
					LatestObject:    &directR[2].Object,
					StaleUpdateTime: 3,
				},
			}

			assert.ElementsMatch(t, want, got)
		}
		f.RClient.On("OverwriteObjects", anyVal, nodes[2], cls, shard, anyVal).
			Return(digestR2, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					LatestObject:    &directR[0].Object,
					StaleUpdateTime: 1,
				},
				{
					LatestObject:    &directR[2].Object,
					StaleUpdateTime: 3,
				},
			}
			assert.ElementsMatch(t, want, got)
		}

		err := finder.CheckConsistency(ctx, All, directR)
		assert.Nil(t, err)
		assert.Equal(t, want, directR)
	})

	t.Run("DigestRead5", func(t *testing.T) {
		var (
			f      = newFakeFactory(cls, shard, nodes)
			finder = f.newFinder("A")
			ids    = []strfmt.UUID{"1", "2", "3", "4", "5"}
			result = []*storobj.Object{
				objectEx(ids[0], 2, shard, "A"),
				objectEx(ids[1], 2, shard, "A"),
				objectEx(ids[2], 3, shard, "A"),
				objectEx(ids[3], 4, shard, "A"),
				objectEx(ids[4], 3, shard, "A"),
			}
			xs = []*storobj.Object{
				objectEx(ids[0], 1, shard, "A"),
				objectEx(ids[1], 1, shard, "A"),
				objectEx(ids[2], 2, shard, "A"),
				objectEx(ids[3], 4, shard, "A"), // latest
				objectEx(ids[4], 2, shard, "A"),
			}
			digestR2 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 2}, // latest
				{ID: ids[1].String(), UpdateTime: 2}, // latest
				{ID: ids[2].String(), UpdateTime: 1},
				{ID: ids[3].String(), UpdateTime: 1},
				{ID: ids[4].String(), UpdateTime: 1},
			}
			digestR3 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[2].String(), UpdateTime: 3}, // latest
				{ID: ids[3].String(), UpdateTime: 1},
				{ID: ids[4].String(), UpdateTime: 3}, // latest
			}
			directR2 = []objects.Replica{
				replica(ids[0], 2, false),
				replica(ids[1], 2, false),
			}
			directR3 = []objects.Replica{
				replica(ids[2], 3, false),
				replica(ids[4], 3, false),
			}
			want = setObjectsConsistency(result, true)
		)
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR3, nil)

		// fetch most recent objects
		f.RClient.On("FetchObjects", anyVal, nodes[1], cls, shard, anyVal).Return(directR2, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]strfmt.UUID)
			assert.ElementsMatch(t, ids[:2], got)
		}
		f.RClient.On("FetchObjects", anyVal, nodes[2], cls, shard, anyVal).Return(directR3, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]strfmt.UUID)
			assert.ElementsMatch(t, []strfmt.UUID{ids[2], ids[4]}, got)
		}

		// repair
		var (
			overwriteR1 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[2].String(), UpdateTime: 2},
				{ID: ids[4].String(), UpdateTime: 2},
			}
			overwriteR2 = []RepairResponse{
				{ID: ids[2].String(), UpdateTime: 1},
				{ID: ids[3].String(), UpdateTime: 1},
				{ID: ids[4].String(), UpdateTime: 1},
			}
			overwriteR3 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[3].String(), UpdateTime: 1},
			}
		)
		f.RClient.On("OverwriteObjects", anyVal, nodes[0], cls, shard, anyVal).
			Return(overwriteR1, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					LatestObject:    &result[0].Object,
					StaleUpdateTime: 1,
				},
				{
					LatestObject:    &result[1].Object,
					StaleUpdateTime: 1,
				},
				{
					LatestObject:    &result[2].Object,
					StaleUpdateTime: 2,
				},
				{
					LatestObject:    &result[4].Object,
					StaleUpdateTime: 2,
				},
			}

			assert.ElementsMatch(t, want, got)
		}

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(overwriteR2, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					LatestObject:    &result[2].Object,
					StaleUpdateTime: 1,
				},
				{
					LatestObject:    &result[3].Object,
					StaleUpdateTime: 1,
				},
				{
					LatestObject:    &result[4].Object,
					StaleUpdateTime: 1,
				},
			}

			assert.ElementsMatch(t, want, got)
		}
		f.RClient.On("OverwriteObjects", anyVal, nodes[2], cls, shard, anyVal).
			Return(overwriteR3, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					LatestObject:    &result[0].Object,
					StaleUpdateTime: 1,
				},
				{
					LatestObject:    &result[1].Object,
					StaleUpdateTime: 1,
				},
				{
					LatestObject:    &result[3].Object,
					StaleUpdateTime: 1,
				},
			}
			assert.ElementsMatch(t, want, got)
		}

		err := finder.CheckConsistency(ctx, All, xs)
		assert.Nil(t, err)
		assert.Equal(t, want, xs)
	})

	t.Run("ChangedObject", func(t *testing.T) {
		var (
			f      = newFakeFactory("C1", shard, nodes)
			finder = f.newFinder("A")
			xs     = []*storobj.Object{
				objectEx(ids[0], 4, shard, "A"),
				objectEx(ids[1], 5, shard, "A"),
				objectEx(ids[2], 6, shard, "A"),
			}
			digestR2 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 4},
				{ID: ids[1].String(), UpdateTime: 2},
				{ID: ids[2].String(), UpdateTime: 3},
			}
			digestR3 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 5},
				{ID: ids[2].String(), UpdateTime: 3},
			}
			digestR4 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 4},
				{ID: ids[1].String(), UpdateTime: 2},
				{ID: ids[2].String(), UpdateTime: 1, Err: "conflict"}, // this one
			}
		)
		want := setObjectsConsistency(xs, true)
		want[2].IsConsistent = false
		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).Return(digestR2, nil)
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).Return(digestR3, nil)

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(digestR4, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					LatestObject:    &xs[1].Object,
					StaleUpdateTime: 2,
				},
				{
					LatestObject:    &xs[2].Object,
					StaleUpdateTime: 3,
				},
			}

			assert.ElementsMatch(t, want, got)
		}
		f.RClient.On("OverwriteObjects", anyVal, nodes[2], cls, shard, anyVal).
			Return(digestR2, nil).
			Once().
			RunFn = func(a mock.Arguments) {
			got := a[4].([]*objects.VObject)
			want := []*objects.VObject{
				{
					LatestObject:    &xs[0].Object,
					StaleUpdateTime: 1,
				},
				{
					LatestObject:    &xs[2].Object,
					StaleUpdateTime: 3,
				},
			}
			assert.ElementsMatch(t, want, got)
		}

		err := finder.CheckConsistency(ctx, All, xs)
		assert.Nil(t, err)
		assert.Equal(t, want, xs)
	})

	t.Run("OverwriteError", func(t *testing.T) {
		var (
			f      = newFakeFactory("C1", shard, nodes)
			finder = f.newFinder("A")
			ids    = []strfmt.UUID{"1", "2", "3"}
			xs     = []*storobj.Object{
				objectEx(ids[0], 2, shard, "A"),
				objectEx(ids[1], 3, shard, "A"),
				objectEx(ids[2], 1, shard, "A"),
			}

			digestR2 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 3}, // latest
				{ID: ids[2].String(), UpdateTime: 1},
			}
			digestR3 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[2].String(), UpdateTime: 4}, // latest
			}
			directR2 = []objects.Replica{
				replica(ids[1], 3, false),
			}
			directR3 = []objects.Replica{
				replica(ids[2], 4, false),
			}
		)

		want := setObjectsConsistency([]*storobj.Object{
			xs[0],
			directR2[0].Object,
			xs[2],
		}, false)
		want[1].IsConsistent = true
		want[1].BelongsToNode = "A"
		want[1].BelongsToShard = shard

		f.RClient.On("DigestObjects", anyVal, nodes[1], cls, shard, ids).
			Return(digestR2, nil).
			Once()
		f.RClient.On("DigestObjects", anyVal, nodes[2], cls, shard, ids).
			Return(digestR3, nil).
			Once()

		// fetch most recent objects
		f.RClient.On("FetchObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(directR2, nil).
			Once()
		f.RClient.On("FetchObjects", anyVal, nodes[2], cls, shard, anyVal).
			Return(directR3, nil).
			Once()
		// repair
		var (
			repairR1 = []RepairResponse{
				{ID: ids[1].String(), UpdateTime: 1},
				{ID: ids[2].String(), UpdateTime: 1},
			}

			repairR2 = []RepairResponse(nil)
			repairR3 = []RepairResponse{
				{ID: ids[0].String(), UpdateTime: 1},
				{ID: ids[1].String(), UpdateTime: 1},
			}
		)
		f.RClient.On("OverwriteObjects", anyVal, nodes[0], cls, shard, anyVal).
			Return(repairR1, nil).
			Once()

		f.RClient.On("OverwriteObjects", anyVal, nodes[1], cls, shard, anyVal).
			Return(repairR2, errAny).
			Once()
		f.RClient.On("OverwriteObjects", anyVal, nodes[2], cls, shard, anyVal).
			Return(repairR3, nil).
			Once()

		err := finder.CheckConsistency(ctx, All, xs)
		assert.Nil(t, err)
		assert.Equal(t, want, xs)
	})
}

func genInputs(node, shard string, updateTime int64, ids []strfmt.UUID) ([]*storobj.Object, []RepairResponse) {
	xs := make([]*storobj.Object, len(ids))
	digestR := make([]RepairResponse, len(ids))
	for i, id := range ids {
		xs[i] = &storobj.Object{
			Object: models.Object{
				ID:                 id,
				LastUpdateTimeUnix: updateTime,
			},
			BelongsToShard: shard,
			BelongsToNode:  node,
		}
		digestR[i] = RepairResponse{ID: ids[i].String(), UpdateTime: updateTime}
	}
	return xs, digestR
}

func setObjectsConsistency(xs []*storobj.Object, isConsistent bool) []*storobj.Object {
	want := make([]*storobj.Object, len(xs))
	for i, x := range xs {
		cp := *x
		cp.IsConsistent = isConsistent
		want[i] = &cp
	}
	return want
}
