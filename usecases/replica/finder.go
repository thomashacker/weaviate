//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replica

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

var (
	// msgCLevel consistency level cannot be achieved
	msgCLevel = "cannot achieve consistency level"
	// errConflictFindDeleted object exists on one replica but is deleted on the other.
	//
	// It depends on the order of operations
	// Created -> Deleted    => It is safe in this case to propagate deletion to all replicas
	// Created -> Deleted -> Created => propagating deletion will result in data lost
	errConflictExistOrDeleted = errors.New("conflict: object has been deleted on another replica")

	// errConflictObjectChanged object changed since last time and cannot be repaired
	errConflictObjectChanged = errors.New("source object changed during repair")

	errReplicas = errors.New("cannot find enough replicas")
	errRepair   = errors.New("read repair error")
	errRead     = errors.New("read error")
)

type (
	// senderReply represent the data received from a sender
	senderReply[T any] struct {
		sender     string // hostname of the sender
		Version    int64  // sender's current version of the object
		Data       T      // the data sent by the sender
		UpdateTime int64  // sender's current update time
		DigestRead bool
	}
	findOneReply senderReply[objects.Replica]
	existReply   struct {
		Sender string
		RepairResponse
	}
)

// Finder finds replicated objects
type Finder struct {
	client   finderClient // needed to commit and abort operation
	resolver *resolver    // host names of replicas
	class    string
	repairer
	log logrus.FieldLogger
}

// NewFinder constructs a new finder instance
func NewFinder(className string,
	stateGetter shardingState, nodeResolver nodeResolver,
	client RClient, l logrus.FieldLogger,
) *Finder {
	cl := finderClient{client}
	return &Finder{
		client: cl,
		resolver: &resolver{
			schema:       stateGetter,
			nodeResolver: nodeResolver,
			class:        className,
		},
		class:    className,
		repairer: repairer{client: cl, class: className},
		log:      l,
	}
}

// GetOne gets object which satisfies the giving consistency
func (f *Finder) GetOne(ctx context.Context,
	l ConsistencyLevel, shard string,
	id strfmt.UUID, props search.SelectProperties, adds additional.Properties,
) (*storobj.Object, error) {
	c := newReadCoordinator[findOneReply](f, shard)
	op := func(ctx context.Context, host string, fullRead bool) (findOneReply, error) {
		if fullRead {
			r, err := f.client.FullRead(ctx, host, f.class, shard, id, props, adds)
			return findOneReply{host, 0, r, r.UpdateTime(), false}, err
		} else {
			xs, err := f.client.DigestReads(ctx, host, f.class, shard, []strfmt.UUID{id})
			var x RepairResponse
			if len(xs) == 1 {
				x = xs[0]
			}
			r := objects.Replica{ID: id, Deleted: x.Deleted}
			return findOneReply{host, x.Version, r, x.UpdateTime, true}, err
		}
	}
	replyCh, state, err := c.Pull(ctx, l, op)
	if err != nil {
		f.log.WithField("op", "pull.one").Error(err)
		return nil, fmt.Errorf("%s %q: %w", msgCLevel, l, errReplicas)
	}
	result := <-f.readOne(ctx, shard, id, replyCh, state)
	if err = result.err; err != nil {
		err = fmt.Errorf("%s %q: %w", msgCLevel, l, err)
	}
	return result.data, err
}

// batchReply represents the data returned by sender
// The returned data may result from a full or digest read request
type batchReply struct {
	// Sender hostname of the sender
	Sender string
	// IsDigest is this reply from a digest read?
	IsDigest bool
	// FullData returned from a full read request
	FullData []objects.Replica
	// DigestData returned from a digest read request
	DigestData []RepairResponse
}

// GetAll gets all objects which satisfy the giving consistency
func (f *Finder) GetAll(ctx context.Context, l ConsistencyLevel, shard string,
	ids []strfmt.UUID,
) ([]*storobj.Object, error) {
	c := newReadCoordinator[batchReply](f, shard)
	op := func(ctx context.Context, host string, fullRead bool) (batchReply, error) {
		if fullRead {
			xs, err := f.client.FullReads(ctx, host, f.class, shard, ids)
			return batchReply{Sender: host, IsDigest: false, FullData: xs}, err
		} else {
			xs, err := f.client.DigestReads(ctx, host, f.class, shard, ids)
			return batchReply{Sender: host, IsDigest: true, DigestData: xs}, err
		}
	}
	replyCh, state, err := c.Pull(ctx, l, op)
	if err != nil {
		f.log.WithField("op", "pull.all").Error(err)
		return nil, fmt.Errorf("%s %q: %w", msgCLevel, l, errReplicas)
	}
	result := <-f.readAll(ctx, shard, ids, replyCh, state)
	if err = result.err; err != nil {
		err = fmt.Errorf("%s %q: %w", msgCLevel, l, err)
	}

	return result.data, err
}

// Exists checks if an object exists which satisfies the giving consistency
func (f *Finder) Exists(ctx context.Context, l ConsistencyLevel, shard string, id strfmt.UUID) (bool, error) {
	c := newReadCoordinator[existReply](f, shard)
	op := func(ctx context.Context, host string, _ bool) (existReply, error) {
		xs, err := f.client.DigestReads(ctx, host, f.class, shard, []strfmt.UUID{id})
		var x RepairResponse
		if len(xs) == 1 {
			x = xs[0]
		}
		return existReply{host, x}, err
	}
	replyCh, state, err := c.Pull(ctx, l, op)
	if err != nil {
		f.log.WithField("op", "pull.exist").Error(err)
		return false, fmt.Errorf("%s %q: %w", msgCLevel, l, errReplicas)
	}
	result := <-f.readExistence(ctx, shard, id, replyCh, state)
	if err = result.err; err != nil {
		err = fmt.Errorf("%s %q: %w", msgCLevel, l, err)
	}
	return result.data, err
}

// NodeObject gets object from a specific node.
// it is used mainly for debugging purposes
func (f *Finder) NodeObject(ctx context.Context,
	nodeName, shard string, id strfmt.UUID,
	props search.SelectProperties, adds additional.Properties,
) (*storobj.Object, error) {
	host, ok := f.resolver.NodeHostname(nodeName)
	if !ok || host == "" {
		return nil, fmt.Errorf("cannot resolve node name: %s", nodeName)
	}
	r, err := f.client.FullRead(ctx, host, f.class, shard, id, props, adds)
	return r.Object, err
}

func (f *Finder) readOne(ctx context.Context,
	shard string,
	id strfmt.UUID,
	ch <-chan simpleResult[findOneReply],
	st rState,
) <-chan result[*storobj.Object] {
	// counters tracks the number of votes for each participant
	resultCh := make(chan result[*storobj.Object], 1)
	go func() {
		defer close(resultCh)
		var (
			votes      = make([]objTuple, 0, len(st.Hosts))
			maxCount   = 0
			contentIdx = -1
		)

		for r := range ch { // len(ch) == st.Level
			resp := r.Response
			if r.Err != nil { // a least one node is not responding
				f.log.WithField("op", "get").WithField("replica", resp.sender).
					WithField("class", f.class).WithField("shard", shard).WithField("uuid", id).Error(r.Err)
				resultCh <- result[*storobj.Object]{nil, errRead}
				return
			}
			if !resp.DigestRead {
				contentIdx = len(votes)
			}
			votes = append(votes, objTuple{resp.sender, resp.UpdateTime, resp.Data, 0, nil})
			for i := range votes { // count number of votes
				if votes[i].UTime == resp.UpdateTime {
					votes[i].ack++
				}
				if maxCount < votes[i].ack {
					maxCount = votes[i].ack
				}
				if maxCount >= st.Level && contentIdx >= 0 {
					resultCh <- result[*storobj.Object]{votes[contentIdx].o.Object, nil}
					return
				}
			}
		}

		obj, err := f.repairOne(ctx, shard, id, votes, st, contentIdx)
		if err == nil {
			resultCh <- result[*storobj.Object]{obj, nil}
			return
		}

		resultCh <- result[*storobj.Object]{nil, errRepair}
		var sb strings.Builder
		for i, c := range votes {
			if i != 0 {
				sb.WriteByte(' ')
			}
			fmt.Fprintf(&sb, "%s:%d", c.sender, c.UTime)
		}
		f.log.WithField("op", "repair_one").WithField("class", f.class).WithField("shard", shard).WithField("uuid", id).WithField("msg", sb.String()).Error(err)
	}()
	return resultCh
}

type vote struct {
	batchReply
	Count []int
	Err   error
}

func (r batchReply) UpdateTimeAt(idx int) int64 {
	if len(r.DigestData) != 0 {
		return r.DigestData[idx].UpdateTime
	}
	return r.FullData[idx].UpdateTime()
}

type _Results result[[]*storobj.Object]

func (f *Finder) readAll(ctx context.Context, shard string, ids []strfmt.UUID, ch <-chan simpleResult[batchReply], st rState) <-chan _Results {
	resultCh := make(chan _Results, 1)

	go func() {
		defer close(resultCh)
		var (
			N = len(ids) // number of requested objects
			// votes counts number of votes per object for each node
			votes      = make([]vote, 0, len(st.Hosts))
			contentIdx = -1 // index of full read reply
		)

		for r := range ch { // len(ch) == st.Level
			resp := r.Response
			if r.Err != nil { // a least one node is not responding
				f.log.WithField("op", "get").WithField("replica", r.Response.Sender).
					WithField("class", f.class).WithField("shard", shard).Error(r.Err)
				resultCh <- _Results{nil, errRead}
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
				resultCh <- _Results{fromReplicas(votes[contentIdx].FullData), nil}
				return
			}
		}
		res, err := f.repairAll(ctx, shard, ids, votes, st, contentIdx)
		if err == nil {
			resultCh <- _Results{res, nil}
		}
		resultCh <- _Results{nil, errRepair}
		f.log.WithField("op", "repair_all").WithField("class", f.class).
			WithField("shard", shard).WithField("uuids", ids).Error(err)
	}()

	return resultCh
}

func (f *Finder) readExistence(ctx context.Context,
	shard string,
	id strfmt.UUID,
	ch <-chan simpleResult[existReply],
	st rState,
) <-chan result[bool] {
	resultCh := make(chan result[bool], 1)
	go func() {
		defer close(resultCh)
		var (
			votes    = make([]boolTuple, 0, len(st.Hosts)) // number of votes per replica
			maxCount = 0
		)

		for r := range ch { // len(ch) == st.Level
			resp := r.Response
			if r.Err != nil { // a least one node is not responding
				f.log.WithField("op", "exists").WithField("replica", resp.Sender).
					WithField("class", f.class).WithField("shard", shard).
					WithField("uuid", id).Error(r.Err)
				resultCh <- result[bool]{false, errRead}
				return
			}

			votes = append(votes, boolTuple{resp.Sender, resp.UpdateTime, resp.RepairResponse, 0, nil})
			for i := range votes { // count number of votes
				if votes[i].UTime == resp.UpdateTime {
					votes[i].ack++
				}
				if maxCount < votes[i].ack {
					maxCount = votes[i].ack
				}
				if maxCount >= st.Level {
					exists := !votes[i].o.Deleted && votes[i].o.UpdateTime != 0
					resultCh <- result[bool]{exists, nil}
					return
				}
			}
		}

		obj, err := f.repairExist(ctx, shard, id, votes, st)
		if err == nil {
			resultCh <- result[bool]{obj, nil}
			return
		}
		resultCh <- result[bool]{false, errRepair}

		var sb strings.Builder
		for i, c := range votes {
			if i != 0 {
				sb.WriteByte(' ')
			}
			fmt.Fprintf(&sb, "%s:%d", c.sender, c.UTime)
		}
		f.log.WithField("op", "repair_exist").WithField("class", f.class).
			WithField("shard", shard).WithField("uuid", id).
			WithField("msg", sb.String()).Error(err)
	}()
	return resultCh
}
