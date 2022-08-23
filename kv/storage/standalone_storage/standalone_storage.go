package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

type StandAloneStorageReader struct {
	dbit engine_util.DBIterator
	txn  *badger.Txn
	db   *badger.DB
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	prefix := []byte(cf + "_")
	cf_key := append(prefix, key[:]...)
	res := make([]byte, 0)

	err := r.db.View(
		func(txn *badger.Txn) error {
			value, err := txn.Get(cf_key)
			if err != nil {
				res = []byte(nil)
			} else {
				res, err = value.ValueCopy(nil)
				if err != nil {
					return err
				}
			}
			return nil
		})
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	if r.dbit == nil {
		// Seek the first position of cf
		r.dbit = engine_util.NewCFIterator(cf, r.txn)
	}

	return r.dbit
}

func (r *StandAloneStorageReader) Close() {
	r.dbit.Close()
	r.txn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).

	//Create & return StandAloneStorage Object
	sas := new(StandAloneStorage)
	return sas
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).

	// Open a badger instance
	opts := badger.DefaultOptions
	opts.Dir = "/tmp/badger"
	opts.ValueDir = "/tmp/badger"
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
		return err
	}
	s.db = db

	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).

	// TODO: Should I abort all the alive transaction here?
	err := s.db.Close()
	if err != nil {
		return err
	}

	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	sasr := &StandAloneStorageReader{
		txn: s.db.NewTransaction(false),
		db:  s.db,
	}
	return sasr, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// txn := s.db.NewTransaction(true)
	// defer txn.Discard()

	for _, b := range batch {
		cf_key := append([]byte(b.Cf()+string("_")), b.Key()[:]...)
		switch b.Data.(type) {
		case storage.Put:
			s.db.Update(func(txn *badger.Txn) error {
				err := txn.Set(cf_key, b.Value())
				return err
			})
		case storage.Delete:
			s.db.Update(func(txn *badger.Txn) error {
				err := txn.Delete(cf_key)
				return err
			})
		}
	}

	return nil
}
