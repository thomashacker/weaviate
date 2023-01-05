package scaling

import (
	"context"
	"io"
	"sort"
	"strconv"

	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
)

const (
	localNode = "N1"
)

var anyVal = mock.Anything // errAny = errors.New("any error")

type fakeFactory struct {
	LocalNode     string
	Nodes         []string
	ShardingState fakeShardingState
	NodeHostMap   map[string]string
	Source        *fakeSource
	Client        *fakeClient
	logger        logrus.FieldLogger
}

func newShardingState(nShard, nNodes, rf int) fakeShardingState {
	if rf > nNodes {
		return nil
	}
	m := make(fakeShardingState)
	for i := 0; i < nShard; i++ {
		replicas := make([]string, rf)
		for j := 0; j < rf; j++ {
			replicas[j] = "N" + strconv.Itoa(j+1)
		}
		m["S"+strconv.Itoa(i+1)] = replicas
	}
	return m
}

func newFakeFactory(nShars, nNodes, rf int) *fakeFactory {
	nodeHostMap := make(map[string]string)
	nodes := make([]string, nNodes)
	for i := 0; i < nNodes; i++ {
		ni := strconv.Itoa(i + 1)
		nodeHostMap["N"+ni] = "H" + ni
		nodes[i] = "N" + ni
	}
	logger, _ := test.NewNullLogger()
	return &fakeFactory{
		LocalNode:     localNode,
		Nodes:         nodes,
		ShardingState: newShardingState(nShars, nNodes, rf),
		NodeHostMap:   nodeHostMap,
		Source:        &fakeSource{},
		Client:        &fakeClient{},
		logger:        logger,
	}
}

func (f fakeFactory) Scaler(dataPath string) *ScaleOutManager {
	nodeResolver := newFakeNodeResolver(f.LocalNode, f.NodeHostMap)
	scaler := NewScaleOutManager(
		nodeResolver,
		f.Source,
		f.Client,
		f.logger,
		dataPath)

	scaler.SetSchemaManager(f.ShardingState)
	return scaler
}

type fakeShardingState map[string][]string

func (f fakeShardingState) ShardingState(class string) *sharding.State {
	if len(f) == 0 {
		return nil
	}
	state := sharding.State{}
	state.Physical = make(map[string]sharding.Physical)
	for shard, nodes := range f {
		state.Physical[shard] = sharding.Physical{BelongsToNodes: nodes}
	}
	state.SetLocalName(localNode)
	return &state
}

// node resolver
type fakeNodeResolver struct {
	NodeName string
	M        map[string]string
}

func newFakeNodeResolver(localNode string, nodeHostMap map[string]string) *fakeNodeResolver {
	return &fakeNodeResolver{NodeName: localNode, M: nodeHostMap}
}

func (r *fakeNodeResolver) NodeHostname(nodeName string) (string, bool) {
	return r.M[nodeName], true
}

func (r *fakeNodeResolver) AllNames() []string {
	xs := make([]string, 0, len(r.M))
	for k := range r.M {
		xs = append(xs, k)
	}
	sort.Strings(xs)
	return xs
}

func (r *fakeNodeResolver) LocalName() string {
	return r.NodeName
}

type fakeSource struct {
	mock.Mock
}

func (s *fakeSource) ReleaseBackup(ctx context.Context, id, class string) error {
	args := s.Called(ctx, id, class)
	return args.Error(0)
}

func (s *fakeSource) ShardsBackup(
	ctx context.Context, id, class string, shards []string,
) (_ backup.ClassDescriptor, err error) {
	args := s.Called(ctx, id, class, shards)
	return args.Get(0).(backup.ClassDescriptor), args.Error(1)
}

type fakeClient struct {
	mock.Mock
}

func (f *fakeClient) PutFile(ctx context.Context, host, class,
	shard, filename string, payload io.ReadSeekCloser,
) error {
	args := f.Called(ctx, host, class, shard, filename, payload)
	return args.Error(0)
}

func (f *fakeClient) CreateShard(ctx context.Context, host, class, name string) error {
	args := f.Called(ctx, host, class, name)
	return args.Error(0)
}

func (f *fakeClient) ReInitShard(ctx context.Context, host, class, shard string,
) error {
	args := f.Called(ctx, host, class, shard)
	return args.Error(0)
}

func (f *fakeClient) IncreaseReplicationFactor(ctx context.Context,
	host, class string, ssBefore, ssAfter *sharding.State,
) error {
	args := f.Called(ctx, host, class, ssBefore, ssAfter)
	return args.Error(0)
}
