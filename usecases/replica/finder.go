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
	// senderReply is a container for the data received from a replica
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
	resolver     *resolver // host names of replicas
	finderStream           // stream of objects
}

// NewFinder constructs a new finder instance
func NewFinder(className string,
	stateGetter shardingState, nodeResolver nodeResolver,
	client RClient, l logrus.FieldLogger,
) *Finder {
	cl := finderClient{client}
	return &Finder{
		resolver: &resolver{
			schema:       stateGetter,
			nodeResolver: nodeResolver,
			class:        className,
		},
		finderStream: finderStream{
			repairer: repairer{
				class:  className,
				client: cl,
			},
			log: l,
		},
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
