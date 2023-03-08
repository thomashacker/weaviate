package replica

import (
	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/usecases/objects"
)

func readSimpleResponses(batchSize int, level int, ch <-chan simpleResult[SimpleResponse]) []error {
	urs := make([]SimpleResponse, 0, level)
	var firstError error
	for x := range ch {
		if x.Err != nil {
			urs = append(urs, x.Response)
			if len(x.Response.Errors) == 0 && firstError == nil {
				firstError = x.Err
			}
		} else {
			level--
			if level == 0 {
				return make([]error, batchSize)
			}
		}
	}
	if level > 0 && firstError == nil {
		firstError = errBroadcast
	}
	return errorsFromSimpleResponses(batchSize, urs, firstError)
}

func readDeletionResponses(batchSize int, level int, ch <-chan simpleResult[DeleteBatchResponse]) []objects.BatchSimpleObject {
	rs := make([]DeleteBatchResponse, 0, level)
	urs := make([]DeleteBatchResponse, 0, level)
	var firstError error
	for x := range ch {
		if x.Err != nil {
			urs = append(urs, x.Response)
			if len(x.Response.Batch) == 0 && firstError == nil {
				firstError = x.Err
			}
		} else {
			level--
			rs = append(rs, x.Response)
			if level == 0 {
				return resultsFromDeletionResponses(batchSize, rs, nil)
			}
		}
	}
	if level > 0 && firstError == nil {
		firstError = errBroadcast
	}
	urs = append(urs, rs...)
	return resultsFromDeletionResponses(batchSize, urs, firstError)
}

func errorsFromSimpleResponses(batchSize int, rs []SimpleResponse, defaultErr error) []error {
	errs := make([]error, batchSize)
	n := 0
	for _, resp := range rs {
		if len(resp.Errors) != batchSize {
			continue
		}
		n++
		for i, err := range resp.Errors {
			if !err.Empty() && errs[i] == nil {
				errs[i] = err.Clone()
			}
		}
	}
	if n == 0 || n != len(rs) {
		for i := range errs {
			if errs[i] == nil {
				errs[i] = defaultErr
			}
		}
	}
	return errs
}

func resultsFromDeletionResponses(batchSize int, rs []DeleteBatchResponse, defaultErr error) []objects.BatchSimpleObject {
	ret := make([]objects.BatchSimpleObject, batchSize)
	n := 0
	for _, resp := range rs {
		if len(resp.Batch) != batchSize {
			continue
		}
		n++
		for i, x := range resp.Batch {
			if !x.Error.Empty() && ret[i].Err == nil {
				ret[i].Err = x.Error.Clone()
			}
			if ret[i].UUID == "" && x.UUID != "" {
				ret[i].UUID = strfmt.UUID(x.UUID)
			}
		}
	}
	if n == 0 || n != len(rs) {
		for i := range ret {
			if ret[i].Err == nil {
				ret[i].Err = defaultErr
			}
		}
	}
	return ret
}
