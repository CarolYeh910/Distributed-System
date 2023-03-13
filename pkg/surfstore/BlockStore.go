package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	val, ok := bs.BlockMap[blockHash.Hash]

	if !ok {
		return nil, fmt.Errorf("Hash %v not found", blockHash.Hash)
	} else {
		return val, nil
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	key := GetBlockHashString(block.BlockData)
	bs.BlockMap[key] = &Block{BlockData: block.BlockData, BlockSize: block.BlockSize}
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	var res []string
	for _, hash := range blockHashesIn.Hashes {
		_, ok := bs.BlockMap[hash]
		if ok {
			res = append(res, hash)
		}
	}
	blockHashesOut := &BlockHashes{Hashes: res}
	return blockHashesOut, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	blockHash := &BlockHashes{}
	for hash := range bs.BlockMap {
		blockHash.Hashes = append(blockHash.Hashes, hash)
	}
	return blockHash, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
