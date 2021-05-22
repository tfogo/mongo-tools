// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package db

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// BufferedBulkInserter implements a bufio.Writer-like design for queuing up
// documents and inserting them in bulk when the given doc limit (or max
// message size) is reached. Must be flushed at the end to ensure that all
// documents are written.
type BufferedInserter struct {
	collection    *mongo.Collection
	docs          []interface{}
	docLimit      int
	docCount      int
	bulkWriteOpts *options.InsertManyOptions
	upsert        bool
}

func newBufferedInserter(collection *mongo.Collection, docLimit int, ordered bool) *BufferedInserter {
	bb := &BufferedInserter{
		collection:    collection,
		bulkWriteOpts: options.InsertMany().SetOrdered(ordered),
		docLimit:      docLimit,
		docs:          make([]interface{}, 0, docLimit),
	}
	return bb
}

// NewOrderedBufferedBulkInserter returns an initialized BufferedBulkInserter for performing ordered bulk writes.
func NewOrderedBufferedInserter(collection *mongo.Collection, docLimit int) *BufferedInserter {
	return newBufferedInserter(collection, docLimit, true)
}

// NewOrderedBufferedBulkInserter returns an initialized BufferedBulkInserter for performing unordered bulk writes.
func NewUnorderedBufferedInserter(collection *mongo.Collection, docLimit int) *BufferedInserter {
	return newBufferedInserter(collection, docLimit, false)
}

func (bb *BufferedInserter) SetOrdered(ordered bool) *BufferedInserter {
	bb.bulkWriteOpts.SetOrdered(ordered)
	return bb
}

func (bb *BufferedInserter) SetBypassDocumentValidation(bypass bool) *BufferedInserter {
	bb.bulkWriteOpts.SetBypassDocumentValidation(bypass)
	return bb
}

// throw away the old bulk and init a new one
func (bb *BufferedInserter) resetBulk() {
	bb.docs = bb.docs[:0]
	bb.docCount = 0
}

// Insert adds a document to the buffer for bulk insertion. If the buffer becomes full, the bulk write is performed, returning
// any error that occurs.
func (bb *BufferedInserter) Insert(doc interface{}) (*mongo.InsertManyResult, error) {
	rawBytes, err := bson.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("bson encoding error: %v", err)
	}

	return bb.InsertRaw(rawBytes)
}

// InsertRaw adds a document, represented as raw bson bytes, to the buffer for bulk insertion. If the buffer becomes full,
// the bulk write is performed, returning any error that occurs.
func (bb *BufferedInserter) InsertRaw(rawBytes bson.Raw) (*mongo.InsertManyResult, error) {
	return bb.addModel(rawBytes)
}

// addModel adds a WriteModel to the buffer. If the buffer becomes full, the bulk write is performed, returning any error
// that occurs.
func (bb *BufferedInserter) addModel(model bson.Raw) (*mongo.InsertManyResult, error) {
	bb.docCount++
	bb.docs = append(bb.docs, model)

	if bb.docCount >= bb.docLimit {
		return bb.Flush()
	}

	return nil, nil
}

// Flush writes all buffered documents in one bulk write and then resets the buffer.
func (bb *BufferedInserter) Flush() (*mongo.InsertManyResult, error) {
	if bb.docCount == 0 {
		return nil, nil
	}

	defer bb.resetBulk()
	fmt.Printf("bb.Flush()\n")
	return bb.collection.InsertMany(context.Background(), bb.docs, bb.bulkWriteOpts)
}
