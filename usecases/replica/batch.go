package replica

import (
	"sort"

	"github.com/weaviate/weaviate/entities/storobj"
)

type batchInput struct {
	Data  []*storobj.Object
	Index []int  // z-index for data
	Oks   []bool // consistency flags
}

type batchPart struct {
	data  []*storobj.Object
	index []int
	Oks   []bool // consistency flags
	node  string
}

func create(xs []*storobj.Object, ds []ShardDesc) batchInput {
	var bi batchInput
	bi.Data = xs
	bi.Index = make([]int, len(xs))
	bi.Oks = make([]bool, len(xs))
	return bi
}

func cluster(bi batchInput, ds []ShardDesc) map[string]batchPart {
	index := bi.Index
	sort.Slice(index, func(i, j int) bool {
		return ds[index[i]].Name < ds[index[j]].Name
	})
	m := make(map[string]batchPart, 16)
	// partition
	n := index[0]
	cur := ds[index[0]]
	j := 0
	for i := 1; i < n; i++ {
		if ds[index[i]].Name == cur.Name {
			continue
		}
		m[cur.Name] = batchPart{bi.Data, index[j:i], bi.Oks, cur.Node}
		j = i
		cur = ds[index[j]]

	}
	m[cur.Name] = batchPart{bi.Data, index[j:], bi.Oks, cur.Node}
	return nil
}
