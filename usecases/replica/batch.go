package replica

import (
	"sort"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/storobj"
)

type batchInput struct {
	Data  []*storobj.Object
	Index []int  // z-index for data
	Oks   []bool // consistency flags
}

func createBatch(xs []*storobj.Object) batchInput {
	var bi batchInput
	bi.Data = xs
	bi.Index = make([]int, len(xs))
	for i := 0; i < len(xs); i++ {
		bi.Index[i] = i
	}
	bi.Oks = make([]bool, len(xs))
	return bi
}

func cluster(bi batchInput) []batchPart {
	index := bi.Index
	data := bi.Data
	sort.Slice(index, func(i, j int) bool {
		return data[index[i]].BelongsToShard < data[index[j]].BelongsToShard
	})
	clusters := make([]batchPart, 0, 16)
	// partition
	cur := data[index[0]]
	j := 0
	for i := 1; i < len(index); i++ {
		if data[index[i]].BelongsToShard == cur.BelongsToShard {
			continue
		}
		clusters = append(clusters, batchPart{
			Shard: cur.BelongsToShard,
			Node:  cur.BelongsToNode, Data: data,
			Index: index[j:i],
			Oks:   bi.Oks,
		})
		j = i
		cur = data[index[j]]

	}
	clusters = append(clusters, batchPart{
		Shard: cur.BelongsToShard,
		Node:  cur.BelongsToNode, Data: data,
		Index: index[j:],
		Oks:   bi.Oks,
	})
	return clusters
}

type batchPart struct {
	Shard string
	Node  string

	Data  []*storobj.Object
	Index []int  // index for data
	Oks   []bool // consistency flags
}

func (b *batchPart) ObjectIDs() []strfmt.UUID {
	xs := make([]strfmt.UUID, len(b.Index))
	for i, idx := range b.Index {
		xs[i] = b.Data[idx].ID()
	}
	return xs
}
