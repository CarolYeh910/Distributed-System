package surfstore

import (
	context "context"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore *MetaStore

	id             int64
	peers          []string
	pendingCommits []*chan bool
	commitIndex    int64
	lastApplied    int64

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	if s.isCrashed {
		return &FileInfoMap{}, ERR_SERVER_CRASHED
	}
	if s.isLeader {
		if !s.FindMajority(ctx) {
			return &FileInfoMap{}, ERR_SERVER_CRASHED
		}
		return s.metaStore.GetFileInfoMap(ctx, empty)
	}
	return &FileInfoMap{}, ERR_NOT_LEADER
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	if s.isCrashed {
		return &BlockStoreMap{}, ERR_SERVER_CRASHED
	}
	if s.isLeader {
		if !s.FindMajority(ctx) {
			return &BlockStoreMap{}, ERR_SERVER_CRASHED
		}
		return s.metaStore.GetBlockStoreMap(ctx, hashes)
	}
	return &BlockStoreMap{}, ERR_NOT_LEADER
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	if s.isCrashed {
		return &BlockStoreAddrs{}, ERR_SERVER_CRASHED
	}
	if s.isLeader {
		if !s.FindMajority(ctx) {
			return &BlockStoreAddrs{}, ERR_SERVER_CRASHED
		}
		return s.metaStore.GetBlockStoreAddrs(ctx, empty)
	}
	return &BlockStoreAddrs{}, ERR_NOT_LEADER
}

func (s *RaftSurfstore) FindMajority(ctx context.Context) bool {
	empty := &emptypb.Empty{}
	succ, err := s.SendHeartbeat(ctx, empty)
	return err == nil && succ.Flag
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	if s.isCrashed {
		return &Version{}, ERR_SERVER_CRASHED
	}
	if !s.isLeader {
		return &Version{}, ERR_NOT_LEADER
	}
	if !s.FindMajority(ctx) {
		return &Version{}, ERR_SERVER_CRASHED
	}

	// append entry to our log
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})
	commitChan := make(chan bool)
	idx := len(s.pendingCommits)
	s.pendingCommits = append(s.pendingCommits, &commitChan)

	// send entry to all followers in parallel
	go s.sendToAllFollowersInParallel(ctx, idx)

	// keep trying indefinitely (even after responding) ** rely on sendheartbeat

	// commit the entry once majority of followers have it in their log
	commit := <-commitChan

	// once committed, apply to the state machine
	if commit {
		s.lastApplied++
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return nil, nil
}

func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context, idx int) {
	// send entry to all my followers and count the replies
	responses := make(chan bool, len(s.peers)-1)
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		go s.sendToFollower(ctx, addr, responses)
	}

	totalResponses := 1
	totalAppends := 1
	commit := false

	// wait in loop for responses
	for {
		result := <-responses
		totalResponses++
		if result {
			totalAppends++
			if totalAppends > len(s.peers)/2 {
				commit = true
				break
			}
		}
		if totalResponses == len(s.peers) {
			break
		}
	}

	*s.pendingCommits[idx] <- commit
	if commit {
		s.commitIndex = int64(idx)
	}
}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, addr string, responses chan bool) {
	dummyAppendEntriesInput := AppendEntryInput{
		Term: s.term,
		// TODO put the right values
		PrevLogTerm:  0,
		PrevLogIndex: s.lastApplied,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}
	if s.lastApplied > 0 {
		dummyAppendEntriesInput.PrevLogTerm = s.log[s.lastApplied].Term
	}

	// TODO check all errors
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return
	}
	client := NewRaftSurfstoreClient(conn)

	output, err := client.AppendEntries(ctx, &dummyAppendEntriesInput)
	// TODO check output
	if err == nil {
		responses <- output.Success
	} else {
		responses <- false
	}
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	output := &AppendEntryOutput{
		ServerId: s.id,
		Term:     s.term,
		Success:  false,
	}
	for s.isCrashed {
		time.Sleep(10 * time.Millisecond)
	}

	if input.Term > s.term {
		s.isLeaderMutex.Lock()
		defer s.isLeaderMutex.Unlock()
		s.isLeader = false
		s.term = input.Term
	} else if input.Term < s.term {
		return output, nil
	}

	// TODO actually check entries
	if input.PrevLogIndex != -1 {
		if int64(len(s.log)) <= input.PrevLogIndex {
			return output, nil
		}
		if s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
			s.log = s.log[:input.PrevLogIndex]
		}
	}
	for i := len(s.log); i < len(input.Entries); i++ {
		s.log = append(s.log, input.Entries[i])
	}

	for s.lastApplied < input.LeaderCommit {
		entry := s.log[s.lastApplied+1]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		s.lastApplied++
	}
	if s.commitIndex < input.LeaderCommit {
		if input.LeaderCommit < s.lastApplied {
			s.commitIndex = input.LeaderCommit
		} else {
			s.commitIndex = s.lastApplied
		}
	}

	output.Success = true
	return output, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isLeaderMutex.Lock()
	defer s.isLeaderMutex.Unlock()
	s.isLeader = true
	s.term++

	// TODO update state
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	dummyAppendEntriesInput := AppendEntryInput{
		Term: s.term,
		// TODO put the right values
		PrevLogTerm:  0,
		PrevLogIndex: s.lastApplied,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}
	if s.lastApplied > 0 {
		dummyAppendEntriesInput.PrevLogTerm = s.log[s.lastApplied].Term
	}
	valid := 1

	// contact all the follower, send some AppendEntries call
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}

		// TODO check all errors
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return &Success{Flag: false}, err
		}
		client := NewRaftSurfstoreClient(conn)

		_, err = client.AppendEntries(ctx, &dummyAppendEntriesInput)
		if err == nil {
			valid++
		}
	}

	return &Success{Flag: valid > len(s.peers)/2}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
