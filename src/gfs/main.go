package main

import (
	"path/filepath"
	"os"
	"fmt"
	"flag"
	"log"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"filesystem"
)

var progName = filepath.Base(os.Args[0])

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", progName)
	fmt.Fprintf(os.Stderr, "  %s SERVER:PORT MOUNTPOINT\n", progName)
	flag.PrintDefaults()
}

func main() {
	log.SetFlags(0)
	log.SetPrefix(progName + ": ")

	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 2 {
		usage()
		os.Exit(2)
	}
	path := flag.Arg(0)
	mountpoint := flag.Arg(1)
	if err := mount(path, mountpoint); err != nil {
		log.Fatal(err)
	}
}

func mount(serverPort, mountpoint string) error {
	c, err := fuse.Mount(mountpoint, fuse.NoAppleDouble(), fuse.NoAppleXattr())
	if err != nil {
		return err
	}
	defer c.Close()

	filesys := filesystem.NewGfs(serverPort)
	if err := fs.Serve(c, filesys); err != nil {
		return err
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		return err
	}

	return nil
}
