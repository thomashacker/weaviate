package replica

import (
	"sort"
	"strconv"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/storobj"
)

func TestBatchInput(t *testing.T) {
	var (
		N    = 9
		ids  = make([]strfmt.UUID, N)
		data = make([]*storobj.Object, N)
		ds   = make([]ShardDesc, N)
	)
	for i := 0; i < N; i++ {
		uuid := strfmt.UUID(strconv.Itoa(i))
		ids[i] = uuid
		data[i] = object(uuid, 1)
		ds[i] = ShardDesc{"S1", "N1"}
	}
	parts := cluster(createBatch(data), ds)
	assert.Len(t, parts, 1)
	assert.Equal(t, parts[0], batchPart{
		Shard: "S1",
		Node:  "N1",
		Data:  data,
		Index: []int{0, 1, 2, 3, 4, 5, 6, 7, 8},
		Oks:   make([]bool, N),
	})
	assert.Equal(t, parts[0].ObjectIDs(), ids)

	ds[0] = ShardDesc{"S2", "N2"}
	ds[2] = ShardDesc{"S2", "N2"}
	ds[3] = ShardDesc{"S2", "N2"}
	ds[5] = ShardDesc{"S2", "N2"}

	parts = cluster(createBatch(data), ds)
	sort.Slice(parts, func(i, j int) bool { return len(parts[i].Index) < len(parts[j].Index) })
	assert.Len(t, parts, 2)
	assert.ElementsMatch(t, parts[0].ObjectIDs(), []strfmt.UUID{ids[0], ids[2], ids[3], ids[5]})
	assert.Len(t, parts[0].Oks, N)
	assert.Equal(t, parts[0].Shard, "S2")
	assert.Equal(t, parts[0].Node, "N2")

	assert.ElementsMatch(t, parts[1].ObjectIDs(), []strfmt.UUID{ids[1], ids[4], ids[6], ids[7], ids[8]})
	assert.Len(t, parts[1].Oks, N)
	assert.Equal(t, parts[1].Shard, "S1")
	assert.Equal(t, parts[1].Node, "N1")
}
