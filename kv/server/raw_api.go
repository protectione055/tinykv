package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	cf := req.GetCf()
	key := req.GetKey()
	res := new(kvrpcpb.RawGetResponse)
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		res.Error = err.Error()
		return res, err
	}
	defer reader.Close()

	// Get the specific value indexed by cf_key
	it := reader.IterCF(cf)
	it.Seek(key)
	if it.Valid() {
		res.Value, err = it.Item().ValueCopy(nil)
		if err != nil {
			res.Error = err.Error()
			return res, err
		}
	} else {
		res.NotFound = true
	}

	return res, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	res := new(kvrpcpb.RawPutResponse)
	cf := req.GetCf()
	key := req.GetKey()
	value := req.GetValue()
	// cf_key := cf + string(key)
	batch := []storage.Modify{
		{Data: storage.Put{
			Key:   key,
			Value: value,
			Cf:    cf,
		},
		},
	}
	err := server.storage.Write(req.GetContext(), batch)
	if err != nil {
		res.Error = err.Error()
		return res, err
	}
	return res, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	res := new(kvrpcpb.RawDeleteResponse)
	cf := req.GetCf()
	key := req.GetKey()
	// cf_key := cf + string(key)
	batch := []storage.Modify{
		{Data: storage.Delete{
			Key: key,
			Cf:  cf,
		},
		},
	}
	err := server.storage.Write(req.GetContext(), batch)
	if err != nil {
		res.Error = err.Error()
		return res, err
	}
	return res, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	cf := req.GetCf()
	start_key := req.GetStartKey()
	res := new(kvrpcpb.RawScanResponse)
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		res.Error = err.Error()
		return res, err
	}
	defer reader.Close()

	// Get the specific value indexed by cf_key
	result_count := 0
	it := reader.IterCF(cf)
	for it.Seek(start_key); it.Valid() && result_count < int(req.GetLimit()); it.Next() {
		kv := new(kvrpcpb.KvPair)
		kv.Key = it.Item().Key()
		kv.Value, err = it.Item().Value()
		if err != nil {
			res.Error = err.Error()
			return res, err
		}
		res.Kvs = append(res.Kvs, kv)
		result_count++
	}

	return res, nil
}
