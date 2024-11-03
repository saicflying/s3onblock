// main.go

package main

import (
    "github.com/gin-gonic/gin"
    "yourapp/server"
    "yourapp/storage"
)

func main() {
    // Initialize the storage
    blockStorage := &storage.InMemoryBlockStorage{}
    if err := blockStorage.Connect(1024 * 1024); err != nil { // 1 MB capacity
        panic(err)
    }

    // Initialize the S3 server
    s3Server := server.NewS3Server(blockStorage)

    // Set up the Gin router
    r := gin.Default()

    // Define S3-compatible routes
    r.POST("/bucket/:bucket", s3Server.CreateBucket)
    r.DELETE("/bucket/:bucket", s3Server.DeleteBucket)
    r.POST("/allocate-metadata/:size", s3Server.AllocateMetadata)
    r.PUT("/bucket/:bucket/object/:key", s3Server.Put

