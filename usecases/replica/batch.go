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

func cluster(bi batchInput, ds []ShardDesc) []batchPart {
	index := bi.Index
	sort.Slice(index, func(i, j int) bool {
		return ds[index[i]].Name < ds[index[j]].Name
	})
	clusters := make([]batchPart, 0, 16)
	// partition
	cur := ds[index[0]]
	j := 0
	for i := 1; i < len(index); i++ {
		if ds[index[i]].Name == cur.Name {
			continue
		}
		clusters = append(clusters, batchPart{cur.Name, cur.Node, bi.Data, index[j:i], bi.Oks})
		j = i
		cur = ds[index[j]]

	}
	clusters = append(clusters, batchPart{cur.Name, cur.Node, bi.Data, index[j:], bi.Oks})
	return clusters
}

type batchPart struct {
	Shard string
	Node  string

	Data  []*storobj.Object
	Index []int
	Oks   []bool // consistency flags
}

func (b *batchPart) ObjectIDs() []strfmt.UUID {
	xs := make([]strfmt.UUID, len(b.Index))
	for i, idx := range b.Index {
		xs[i] = b.Data[idx].ID()
	}
	return xs
}
