package replica

import (
	"context"
	"fmt"
	"sort"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"golang.org/x/sync/errgroup"
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

func (f *Finder) checkShardConsistency(ctx context.Context,
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
		f.log.WithField("op", "shard_consistency.pull").
			WithField("shard", batch.Shard).Error(err)
		return nil, fmt.Errorf("%s %q: %w", msgCLevel, l, errReplicas)
	}
	result := <-f.readBatchPart(ctx, batch, ids, replyCh, state)
	if err = result.Err; err != nil {
		err = fmt.Errorf("%s %q: %w", msgCLevel, l, err)
	}

	return result.Value, err
}

// readBatchPart reads in replicated objects specified by their ids
// readAll reads in replicated objects specified by their ids
func (f *finderStream) readBatchPart(ctx context.Context,
	batch batchPart,
	ids []strfmt.UUID,
	ch <-chan _Result[batchReply], st rState,
) <-chan batchResult {
	resultCh := make(chan batchResult, 1)

	go func() {
		defer close(resultCh)
		var (
			N = len(ids) // number of requested objects
			// votes counts number of votes per object for each node
			votes      = make([]vote, 0, len(st.Hosts))
			contentIdx = -1 // index of full read reply
		)

		for r := range ch { // len(ch) == st.Level
			resp := r.Value
			if r.Err != nil { // at least one node is not responding
				f.log.WithField("op", "read_batch.get").WithField("replica", r.Value.Sender).
					WithField("class", f.class).WithField("shard", batch.Shard).Error(r.Err)
				resultCh <- batchResult{nil, errRead}
				return
			}
			if !resp.IsDigest {
				contentIdx = len(votes)
			}

			votes = append(votes, vote{resp, make([]int, N), nil})
			M := 0
			for i := 0; i < N; i++ {
				max := 0
				lastTime := resp.UpdateTimeAt(i)

				for j := range votes { // count votes
					if votes[j].UpdateTimeAt(i) == lastTime {
						votes[j].Count[i]++
					}
					if max < votes[j].Count[i] {
						max = votes[j].Count[i]
					}
				}
				if max >= st.Level {
					M++
				}
			}

			if M == N {
				for _, idx := range batch.Index {
					batch.Data[idx].IsConsistent = true
				}
				resultCh <- batchResult{fromReplicas(votes[contentIdx].FullData), nil}
				return
			}
		}
		res, err := f.repairBatchPart(ctx, batch.Shard, ids, votes, st, contentIdx)
		if err != nil {
			resultCh <- batchResult{nil, errRepair}
			f.log.WithField("op", "repair_batch").WithField("class", f.class).
				WithField("shard", batch.Shard).WithField("uuids", ids).Error(err)
			return
		}
		// count votes
		maxCount := len(votes)
		sum := votes[0].Count
		nc := 0
		for _, vote := range votes[1:] {
			for i, n := range vote.Count {
				sum[i] += n
			}
		}
		for i, n := range sum {
			if n == maxCount {
				batch.Data[batch.Index[i]].IsConsistent = true
				nc++
			}
		}

		resultCh <- batchResult{res, nil}
	}()

	return resultCh
}

// repairAll repairs objects when reading them ((use in combination with Finder::GetAll)
func (r *repairer) repairBatchPart(ctx context.Context,
	shard string,
	ids []strfmt.UUID,
	votes []vote,
	st rState,
	contentIdx int,
) ([]*storobj.Object, error) {
	var (
		result     = make([]*storobj.Object, len(ids)) // final result
		lastTimes  = make([]iTuple, len(ids))          // most recent times
		ms         = make([]iTuple, 0, len(ids))       // mismatches
		nDeletions = 0
		cl         = r.client
		nVotes     = len(votes)
	)

	// find most recent objects
	for i, x := range votes[contentIdx].FullData {
		lastTimes[i] = iTuple{S: contentIdx, O: i, T: x.UpdateTime(), Deleted: x.Deleted}
		votes[contentIdx].Count[i] = nVotes // reuse Count[] to check consistency
	}
	for i, vote := range votes {
		if i != contentIdx {
			for j, x := range vote.DigestData {
				deleted := lastTimes[j].Deleted || x.Deleted
				if x.UpdateTime > lastTimes[j].T {
					lastTimes[j] = iTuple{S: i, O: j, T: x.UpdateTime}
				}
				lastTimes[j].Deleted = deleted
				votes[i].Count[j] = nVotes
			}
		}
	}
	// find missing content (diff)
	for i, p := range votes[contentIdx].FullData {
		if lastTimes[i].Deleted { // conflict
			nDeletions++
			result[i] = nil
			votes[contentIdx].Count[i] = 0
		} else if contentIdx != lastTimes[i].S {
			ms = append(ms, lastTimes[i])
		} else {
			result[i] = p.Object
		}
	}
	if len(ms) > 0 { // fetch most recent objects
		// partition by hostname
		sort.SliceStable(ms, func(i, j int) bool { return ms[i].S < ms[j].S })
		partitions := make([]int, 0, len(votes))
		pre := ms[0].S
		for i, y := range ms {
			if y.S != pre {
				partitions = append(partitions, i)
				pre = y.S
			}
		}
		partitions = append(partitions, len(ms))

		// concurrent fetches
		gr, ctx := errgroup.WithContext(ctx)
		start := 0
		for _, end := range partitions { // fetch diffs
			rid := ms[start].S
			receiver := votes[rid].Sender
			query := make([]strfmt.UUID, end-start)
			for j := 0; start < end; start++ {
				query[j] = ids[ms[start].O]
				j++
			}
			start := start
			gr.Go(func() error {
				resp, err := cl.FullReads(ctx, receiver, r.class, shard, query)
				if err != nil {
					return err
				}
				for i, n := 0, len(query); i < n; i++ {
					idx := ms[start-n+i].O
					if lastTimes[idx].T != resp[i].UpdateTime() {
						votes[rid].Count[idx]--
					} else {
						result[idx] = resp[i].Object
					}
				}
				return nil
			})

		}
		if err := gr.Wait(); err != nil {
			return nil, err
		}
	}

	// concurrent repairs
	// TODO catch directCandidate.Addr == "" (see rState )
	gr, ctx := errgroup.WithContext(ctx)
	for rid, vote := range votes {
		query := make([]*objects.VObject, 0, len(ids)/2)
		m := make(map[string]int, len(ids)/2) //
		for j, x := range lastTimes {
			if cTime := vote.UpdateTimeAt(j); x.T != cTime && !x.Deleted && vote.Count[j] == nVotes {
				query = append(query, &objects.VObject{LatestObject: &result[j].Object, StaleUpdateTime: cTime})
				m[string(result[j].ID())] = j
			}
		}
		if len(query) == 0 {
			continue
		}
		receiver := vote.Sender
		rid := rid
		gr.Go(func() error {
			rs, err := cl.Overwrite(ctx, receiver, r.class, shard, query)
			if err != nil {
				for _, idx := range m {
					votes[rid].Count[idx]--
				}
				return nil
			}
			for _, r := range rs {
				if r.Err != "" {
					if idx, ok := m[r.ID]; ok {
						votes[rid].Count[idx]--
					}
				}
			}
			return nil
		})
	}

	return result, gr.Wait()
}
