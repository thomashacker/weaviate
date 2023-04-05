package replica

import (
	"context"
	"fmt"
	"sort"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

type batchInput struct {
	Data  []*storobj.Object
	Index []int // z-index for data
}

func createBatch(xs []*storobj.Object) batchInput {
	var bi batchInput
	bi.Data = xs
	bi.Index = make([]int, len(xs))
	for i := 0; i < len(xs); i++ {
		bi.Index[i] = i
	}
	return bi
}

// cluster data object by shard
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
		})
		j = i
		cur = data[index[j]]

	}
	clusters = append(clusters, batchPart{
		Shard: cur.BelongsToShard,
		Node:  cur.BelongsToNode, Data: data,
		Index: index[j:],
	})
	return clusters
}

type batchPart struct {
	Shard string
	Node  string

	Data  []*storobj.Object
	Index []int // index for data
}

func (b *batchPart) ObjectIDs() []strfmt.UUID {
	xs := make([]strfmt.UUID, len(b.Index))
	for i, idx := range b.Index {
		xs[i] = b.Data[idx].ID()
	}
	return xs
}

func (b *batchPart) Extract() ([]objects.Replica, []strfmt.UUID) {
	xs := make([]objects.Replica, len(b.Index))
	ys := make([]strfmt.UUID, len(b.Index))

	for i, idx := range b.Index {
		p := b.Data[idx]
		xs[i] = objects.Replica{ID: p.ID(), Deleted: false, Object: p}
		ys[i] = p.ID()
	}
	return xs, ys
}

func (f *Finder) CheckShardConsistency(ctx context.Context,
	l ConsistencyLevel,
	batch batchPart,
) ([]*storobj.Object, error) {
	var (
		c         = newReadCoordinator[batchReply](f, batch.Shard)
		shard     = batch.Shard
		data, ids = batch.Extract()
	)
	op := func(ctx context.Context, host string, fullRead bool) (batchReply, error) {
		if fullRead {
			return batchReply{Sender: host, IsDigest: false, FullData: data}, nil
		} else {
			xs, err := f.client.DigestReads(ctx, host, f.class, shard, ids)
			return batchReply{Sender: host, IsDigest: true, DigestData: xs}, err
		}
	}

	replyCh, state, err := c.Pull(ctx, l, op, batch.Node)
	if err != nil {
		f.log.WithField("op", "pull.all").Error(err)
		return nil, fmt.Errorf("%s %q: %w", msgCLevel, l, errReplicas)
	}
	result := <-f.readAll(ctx, shard, ids, replyCh, state)
	if err = result.Err; err != nil {
		err = fmt.Errorf("%s %q: %w", msgCLevel, l, err)
	}

	return result.Value, err
}
