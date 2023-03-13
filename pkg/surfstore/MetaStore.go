package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	InfoMap := &FileInfoMap{FileInfoMap: m.FileMetaMap}
	return InfoMap, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	cur, ok := m.FileMetaMap[fileMetaData.Filename]
	version := &Version{Version: -1}

	if !ok || fileMetaData.Version == cur.Version+1 {
		fname := fileMetaData.Filename
		m.FileMetaMap[fname] = &FileMetaData{}
		m.FileMetaMap[fname].Filename = fname
		m.FileMetaMap[fname].Version = fileMetaData.Version
		m.FileMetaMap[fname].BlockHashList = fileMetaData.BlockHashList
		version.Version = fileMetaData.Version
	}
	return version, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	bsMap := &BlockStoreMap{BlockStoreMap: map[string]*BlockHashes{}}
	for _, hash := range blockHashesIn.Hashes {
		server := m.ConsistentHashRing.GetResponsibleServer(hash)
		_, ok := bsMap.BlockStoreMap[server]
		if !ok {
			bsMap.BlockStoreMap[server] = &BlockHashes{Hashes: []string{}}
		}
		bsMap.BlockStoreMap[server].Hashes = append(bsMap.BlockStoreMap[server].Hashes, hash)
	}
	return bsMap, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	addrs := &BlockStoreAddrs{}
	addrs.BlockStoreAddrs = m.BlockStoreAddrs
	return addrs, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
