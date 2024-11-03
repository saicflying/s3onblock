// storage/block_storage.go

package storage

import (
    "errors"
    "sync"
)

// BlockStorage defines the interface for block storage operations
type BlockStorage interface {
    Connect(capacity int64) error
    AllocateBlockStorageCapacity(size int64, purpose string) error
    Write(bucket, key string, offset int64, data []byte) error
    Read(bucket, key string, offset int64, length int) ([]byte, error)
}

// InMemoryBlockStorage is a simple in-memory implementation of BlockStorage
type InMemoryBlockStorage struct {
    totalCapacity int64
    allocated     int64
    data          map[string]map[string][]byte // bucket -> key -> data
    mu            sync.Mutex
}

// Connect initializes the storage with a specified total capacity
func (s *InMemoryBlockStorage) Connect(capacity int64) error {
    if capacity <= 0 {
        return errors.New("invalid capacity")
    }
    s.totalCapacity = capacity
    s.data = make(map[string]map[string][]byte)
    return nil
}

// AllocateBlockStorageCapacity allocates capacity for specified purposes
func (s *InMemoryBlockStorage) AllocateBlockStorageCapacity(size int64, purpose string) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    if size <= 0 || size+s.allocated > s.totalCapacity {
        return errors.New("invalid size for storage allocation")
    }

    s.allocated += size
    return nil
}

// Write writes data to the storage in a specific bucket and key at the specified offset
func (s *InMemoryBlockStorage) Write(bucket, key string, offset int64, data []byte) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    if _, exists := s.data[bucket]; !exists {
        s.data[bucket] = make(map[string][]byte)
    }

    if _, exists := s.data[bucket][key]; !exists {
        s.data[bucket][key] = make([]byte, s.totalCapacity-s.allocated) // Remaining capacity for object data
    }

    if offset < 0 || offset+int64(len(data)) > int64(len(s.data[bucket][key])) {
        return errors.New("write exceeds storage capacity")
    }

    copy(s.data[bucket][key][offset:], data)
    return nil
}

// Read reads data from the storage in a specific bucket and key at the specified offset
func (s *InMemoryBlockStorage) Read(bucket, key string, offset int64, length int) ([]byte, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    if _, exists := s.data[bucket]; !exists {
        return nil, errors.New("bucket does not exist")
    }

    data, exists := s.data[bucket][key]
    if !exists {
        return nil, errors.New("object does not exist")
    }

    if offset < 0 || offset+int64(length) > int64(len(data)) {
        return nil, errors.New("read exceeds storage capacity")
    }

    result := make([]byte, length)
    copy(result, data[offset:])
    return result, nil
}

