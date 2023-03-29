package replica

import (
	"strconv"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/storobj"
)

func TestBatchInput(t *testing.T) {
	var (
		N    = 10
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
	assert.Equal(t, parts[0].ObjectIDs(), ids)
}
