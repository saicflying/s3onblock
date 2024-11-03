package storage 

// Storage defines the methods for our storage layer.
type BlockStorage interface {
    Connect(capacity int64) error
    Write(bucket, key string, offset int64, data []byte) error
    Read(bucket, key string, offset int64, length int) ([]byte, error)
}
