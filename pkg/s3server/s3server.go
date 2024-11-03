// server/s3_server.go

package server

import (
    "encoding/xml"
    "net/http"
    "strconv"
    "sync"
    "yourapp/storage"
    "github.com/gin-gonic/gin"
)

type S3Server struct {
    storage storage.BlockStorage
    buckets sync.Map // Map to keep track of buckets
}

// NewS3Server creates a new S3 server
func NewS3Server(storage storage.BlockStorage) *S3Server {
    return &S3Server{storage: storage}
}

// CreateBucket handles bucket creation
func (s *S3Server) CreateBucket(c *gin.Context) {
    bucket := c.Param("bucket")

    // Check if the bucket already exists
    if _, loaded := s.buckets.LoadOrStore(bucket, struct{}{}); loaded {
        c.String(http.StatusConflict, "Bucket already exists")
        return
    }

    // Allocate space for bucket metadata
    if err := s.storage.AllocateBlockStorageCapacity(1024, "bucket_metadata"); err != nil { // Example size
        c.String(http.StatusInternalServerError, "Error allocating metadata capacity: %v", err)
        return
    }

    // Store bucket metadata
    metadata := []byte("<Bucket><Name>" + bucket + "</Name></Bucket>")
    if err := s.storage.Write("buckets", bucket, 0, metadata); err != nil {
        c.String(http.StatusInternalServerError, "Error storing bucket metadata: %v", err)
        return
    }

    c.String(http.StatusOK, "")
}

// DeleteBucket handles bucket deletion
func (s *S3Server) DeleteBucket(c *gin.Context) {
    bucket := c.Param("bucket")

    // Attempt to delete the bucket
    if _, loaded := s.buckets.LoadAndDelete(bucket); !loaded {
        c.String(http.StatusNotFound, "Bucket does not exist")
        return
    }

    c.String(http.StatusNoContent, "")
}

// PutObject handles file uploads
func (s *S3Server) PutObject(c *gin.Context) {
    bucket := c.Param("bucket")
    key := c.Param("key")
    offset, _ := strconv.ParseInt(c.Query("offset"), 10, 64)

    // Check if the bucket exists
    if _, ok := s.buckets.Load(bucket); !ok {
        c.String(http.StatusNotFound, "Bucket does not exist")
        return
    }

    // Read the uploaded file
    file, err := c.FormFile("file")
    if err != nil {
        c.String(http.StatusBadRequest, "Error retrieving file: %v", err)
        return
    }

    openedFile, err := file.Open()
    if err != nil {
        c.String(http.StatusInternalServerError, "Failed to open file: %v", err)
        return
    }
    defer openedFile.Close()

    data := make([]byte, file.Size)
    if _, err := openedFile.Read(data); err != nil {
        c.String(http.StatusInternalServerError, "Failed to read file: %v", err)
        return
    }

    // Allocate space for object metadata
    if err := s.storage.AllocateBlockStorageCapacity(512, "object_metadata"); err != nil { // Example size
        c.String(http.StatusInternalServerError, "Error allocating object metadata capacity: %v", err)
        return
    }

    // Store object metadata
    metadata := []byte("<Object><Key>" + key + "</Key><Size>" + strconv.FormatInt(int64(len(data)), 10) + "</Size></Object>")
    if err := s.storage.Write("objects", key, 0, metadata); err != nil {
        c.String(http.StatusInternalServerError, "Error storing object metadata: %v", err)
        return
    }

    // Write the actual object data
    if err := s.storage.Write(bucket, key, offset, data); err != nil {
        c.String(http.StatusInternalServerError, "Error writing to storage: %v", err)
        return
    }

    // Return an XML response with the object location
    c.XML(http.StatusOK, gin.H{
        "Location": c.Request.URL.String(),
    })
}

// GetObject handles data retrieval
func (s *S3Server) GetObject(c *gin.Context) {
    bucket := c.Param("bucket")
    key := c.Param("key")
    offset, _ := strconv.ParseInt(c.Query("offset"), 10, 64)
    length := 1024 // Default length

    if lengthStr := c.Query("length"); lengthStr != "" {
        if lenValue, err := strconv.Atoi(lengthStr); err == nil {
            length = lenValue
        }
    }

    // Check if the bucket exists
    if _, ok := s.buckets.Load(bucket); !ok {
        c.XML(http.StatusNotFound, gin.H{"Code": "NoSuchBucket", "Message": "The specified bucket does not exist."})
        return
    }

    data, err := s.storage.Read(bucket, key, offset, length)
    if err != nil {
        c.XML(http.StatusNotFound, gin.H{"Code": "NoSuchKey", "Message": "The specified key does not exist."})
        return
    }

    c.Data(http.StatusOK, "application/octet-stream", data)
}

// ListBuckets handles listing all buckets
func (s *S3Server) ListBuckets(c *gin.Context) {
    type Bucket struct {
        Name string `xml:"Name"`
    }
    type ListBucketsResponse struct {
        XMLName xml.Name `xml:"ListAllMyBucketsResult"`
        Buckets []Bucket  `xml:"Buckets>Bucket"`
        Owner   struct {
            ID string `xml:"ID"`
        } `xml:"Owner"`
    }

    var response ListBucketsResponse
    s.buckets.Range(func(key, value interface{}) bool {
        response.Buckets = append(response.Buckets, Bucket{Name: key.(string)})
        return true
    })

    c.XML(http.StatusOK, response)
}

