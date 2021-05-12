// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

// Main package for the mongorestore tool.
package main

import (
	"runtime"
	"runtime/pprof"

	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/signals"
	"github.com/mongodb/mongo-tools/common/util"
	"github.com/mongodb/mongo-tools/mongorestore"

	"os"
)

var (
	VersionStr = "built-without-version-string"
	GitCommit  = "build-without-git-commit"
)

func main() {
	opts, err := mongorestore.ParseOptions(os.Args[1:], VersionStr, GitCommit)

	if err != nil {
		log.Logvf(log.Always, "error parsing command line options: %s", err.Error())
		log.Logvf(log.Always, util.ShortUsage("mongorestore"))
		os.Exit(util.ExitFailure)
	}

	// print help or version info, if specified
	if opts.PrintHelp(false) {
		return
	}

	if opts.PrintVersion() {
		return
	}

	if opts.CPUProfile != "" {
		f, err := os.Create(opts.CPUProfile)
		if err != nil {
			log.Logvf(log.Always, "could not create CPU profile: ", err)
			os.Exit(util.ExitFailure)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Logvf(log.Always, "could not start CPU profile: ", err)
			os.Exit(util.ExitFailure)
		}
		defer pprof.StopCPUProfile()
	}

	restore, err := mongorestore.New(opts)
	if err != nil {
		log.Logvf(log.Always, err.Error())
		os.Exit(util.ExitFailure)
	}
	defer restore.Close()

	finishedChan := signals.HandleWithInterrupt(restore.HandleInterrupt)
	defer close(finishedChan)

	result := restore.Restore()
	if result.Err != nil {
		log.Logvf(log.Always, "Failed: %v", result.Err)
	}

	if restore.ToolOptions.WriteConcern.Acknowledged() {
		log.Logvf(log.Always, "%v document(s) restored successfully. %v document(s) failed to restore.", result.Successes, result.Failures)
	} else {
		log.Logvf(log.Always, "done")
	}

	if opts.MemProfile != "" {
		f, err := os.Create(opts.MemProfile)
		if err != nil {
			log.Logvf(log.Always, "could not create memory profile: ", err)
			os.Exit(util.ExitFailure)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Logvf(log.Always, "could not write memory profile: ", err)
			os.Exit(util.ExitFailure)
		}
	}

	if result.Err != nil {
		os.Exit(util.ExitFailure)
	}
	os.Exit(util.ExitSuccess)
}
