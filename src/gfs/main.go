package main

import (
	"path/filepath"
	"os"
	"fmt"
	"flag"
	"log"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"github.com/gemfire/geode-go-client/connector"
	geode "github.com/gemfire/geode-go-client"
	"net"
)

const METADATA_REGION = "metadata"
const BLOCKS_REGION = "blocks"

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
	c, err := fuse.Mount(mountpoint, fuse.NoAppleDouble())
	if err != nil {
		return err
	}
	defer c.Close()

	filesys := NewGfs(serverPort)
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

type GFS struct {
	client *geode.Client
}

func NewGfs(serverPort string) *GFS {
	c, err := net.Dial("tcp", serverPort)
	if err != nil {
		panic(err)
	}

	p := connector.NewPool(c)
	conn := connector.NewConnector(p)
	conn.Handshake()
	client := geode.NewGeodeClient(conn)

	return &GFS{client: client}
}

var _ fs.FS = (*GFS)(nil)

func (f *GFS) Root() (fs.Node, error) {
	n := &Dir{
		client: f.client,
		name:   "/",
	}
	return n, nil
}

type Dir struct {
	client *geode.Client
	name   string
}

var _ fs.Node = (*Dir)(nil)

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0755
	return nil
}

var _ = fs.NodeRequestLookuper(&Dir{})

func (d *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	v := &INode{}
	fuse.Debug(fmt.Sprintf("--->>> req: %+v", req))
	_, err := d.client.Get(METADATA_REGION, req.Name, v)
	fuse.Debug(fmt.Sprintf("--->>> get name: %+v", req.Name))
	fuse.Debug(fmt.Sprintf("--->>> get value: %+v", v))
	fuse.Debug(fmt.Sprintf("--->>> get err: %v", err))
	if err != nil {
		return nil, err
	}

	if v.Name == "" {
		return nil, fuse.ENOENT
	}

	file := &File{
		inode: v,
		client: d.client,
	}

	return file, nil
}

var _ = fs.HandleReadDirAller(&Dir{})

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	fuse.Debug("Calling ReadDirAll")
	return nil, fuse.ENOENT
}

type File struct {
	inode  *INode
	client *geode.Client
}

var _ fs.Node = (*File)(nil)

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	//zipAttr(f.file, a)
	return nil
}

var _ = fs.NodeCreater(&Dir{})

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	v := &INode{
		Name: req.Name,
		Parent: d.name,
	}
	err := d.client.Put(METADATA_REGION, req.Name, v)
	if err != nil {
		return nil, nil, err
	}

	i := &File{inode: v}

	return i, nil, nil
}

var _ = fs.NodeOpener(&File{})

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	//r, err := f.file.Open()
	//if err != nil {
	//	return nil, err
	//}
	//individual entries inside a zip file are not seekable
	//resp.Flags |= fuse.OpenNonSeekable

	handle := &FileHandle{
		inode: f.inode,
		client: f.client,
	}

	resp.Flags |= fuse.OpenNonSeekable

	fuse.Debug(fmt.Sprintf("--->>> Open: %+v", handle))

	return handle, nil
}

var _ = fs.NodeRemover(&Dir{})

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	return d.client.Remove(METADATA_REGION, req.Name)
}

type FileHandle struct {
	//r     io.ReadCloser
	inode *INode
	client *geode.Client
}

var _ fs.Handle = (*FileHandle)(nil)

//var _ fs.HandleReleaser = (*FileHandle)(nil)

//func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
//	return fh.r.Close()
//}

var _ fs.HandleReadAller = (*FileHandle)(nil)

func (fh *FileHandle) ReadAll(ctx context.Context) ([]byte, error) {
	rawData, err := fh.client.Get(BLOCKS_REGION, fh.inode.Name)
	if err != nil {
		return nil, err
	}

	fuse.Debug(fmt.Sprintf("--->>> ReadAll: %+v", rawData))

	return rawData.([]byte), nil
}

//var _ fs.HandleReader = (*FileHandle)(nil)
//
//func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
//	rawData, err := fh.client.Get(BLOCKS_REGION, fh.inode.Name)
//	if err != nil {
//		return err
//	}
//
//	fuse.Debug(fmt.Sprintf("--->>> Read: %+v", rawData))
//
//	resp.Data = rawData.([]byte)
//	return nil
//}

var _ fs.HandleWriter = (*FileHandle)(nil)

func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	err := fh.client.Put(BLOCKS_REGION, fh.inode.Name, req.Data)
	resp.Size = len(req.Data)

	return err
}
