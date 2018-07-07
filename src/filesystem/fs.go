package filesystem

import (
	"os"
	"fmt"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"github.com/gemfire/geode-go-client/connector"
	geode "github.com/gemfire/geode-go-client"
	"net"
	"sync"
)

const METADATA_REGION = "metadata"
const BLOCKS_REGION = "blocks"


type GFS struct {
	Client   *geode.Client
	sync.RWMutex
	handleId uint64
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

	return &GFS{Client: client}
}

func (g *GFS) getNewHandleId() fuse.HandleID {
	g.Lock()
	defer g.Unlock()
	g.handleId += 1

	return fuse.HandleID(g.handleId)
}

var _ fs.FS = (*GFS)(nil)

func (g *GFS) Root() (fs.Node, error) {
	n := &Dir{
		gfs: g,
		name:   "/",
	}
	return n, nil
}

type Dir struct {
	gfs    *GFS
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
	ok, err := d.gfs.Client.Get(METADATA_REGION, req.Name, v)
	fuse.Debug(fmt.Sprintf("--->>> get ok: %+v", ok))
	fuse.Debug(fmt.Sprintf("--->>> get name: %+v", req.Name))
	fuse.Debug(fmt.Sprintf("--->>> get value: %+v", v))
	fuse.Debug(fmt.Sprintf("--->>> get err: %v", err))
	if err != nil {
		return nil, err
	}

	if ok == nil {
		return nil, fuse.ENOENT
	}

	file := &File{
		inode: v,
		gfs: d.gfs,
	}

	resp.Attr.Size = v.Size
	resp.Attr.Mode = v.Mode

	return file, nil
}

var _ fs.HandleReadDirAller = (*Dir)(nil)

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	fuse.Debug("---->>>> calling Dir.ReadDirAll")
	q := d.gfs.Client.Query(fmt.Sprintf("select * from /metadata where parent = '%s'", d.name))
	q.Reference = &INode{}

	inodes, err := d.gfs.Client.QueryForListResult(q)
	if err != nil {
		return nil, err
	}

	dirents := make([]fuse.Dirent, 0)
	for _, i := range inodes {
		node := i.(*INode)
		d := fuse.Dirent{
			Name: node.Name,
			Type: fuse.DT_File,
		}
		dirents = append(dirents, d)
	}

	return dirents, nil
}

//var _ = fs.HandleReadDirAller(&Dir{})
//
//func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
//	fuse.Debug("Calling ReadDirAll")
//	return nil, fuse.ENOENT
//}

type File struct {
	inode  *INode
	gfs *GFS
}

var _ fs.Node = (*File)(nil)

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	//zipAttr(f.file, a)
	a.Size = f.inode.Size
	a.Mode = f.inode.Mode

	return nil
}

var _ = fs.NodeCreater(&Dir{})

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	v := &INode{
		Name: req.Name,
		Parent: d.name,
		Mode: req.Mode,
	}
	err := d.gfs.Client.Put(METADATA_REGION, req.Name, v)
	if err != nil {
		return nil, nil, err
	}

	node := &File{inode: v}
	handle := &FileHandle{
		inode: v,
		gfs: d.gfs,
	}

	resp.Handle = d.gfs.getNewHandleId()

	return node, handle, nil
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
		gfs: f.gfs,
	}

	resp.Flags |= fuse.OpenNonSeekable
	resp.Handle = f.gfs.getNewHandleId()

	fuse.Debug(fmt.Sprintf("--->>> Open: %s %+v", f.inode.Name, handle))

	return handle, nil
}

var _ = fs.NodeRemover(&Dir{})

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	return d.gfs.Client.Remove(METADATA_REGION, req.Name)
}

type FileHandle struct {
	//r     io.ReadCloser
	inode *INode
	gfs *GFS
}

var _ fs.Handle = (*FileHandle)(nil)

//var _ fs.HandleReleaser = (*FileHandle)(nil)

//func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
//	return fh.r.Close()
//}

var _ fs.HandleReadAller = (*FileHandle)(nil)

func (fh *FileHandle) ReadAll(ctx context.Context) ([]byte, error) {
	fuse.Debug("--->>> ReadAll")
	rawData, err := fh.gfs.Client.Get(BLOCKS_REGION, fh.inode.Name)
	if err != nil {
		return nil, err
	}

	fuse.Debug(fmt.Sprintf("--->>> ReadAll: %+v", rawData))

	return rawData.([]byte), nil
}

var _ fs.HandleReader = (*FileHandle)(nil)

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	rawData, err := fh.gfs.Client.Get(BLOCKS_REGION, fh.inode.Name)
	if err != nil {
		return err
	}

	fuse.Debug(fmt.Sprintf("--->>> Read: %+v", rawData))
	resp.Data = rawData.([]byte)

	return nil
}

var _ fs.HandleWriter = (*FileHandle)(nil)

func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	fuse.Debug(fmt.Sprintf("--->>> writing to %s", fh.inode.Name))
	err := fh.gfs.Client.Put(BLOCKS_REGION, fh.inode.Name, req.Data)
	resp.Size = len(req.Data)

	inode := &INode{}
	ok, err := fh.gfs.Client.Get(METADATA_REGION, fh.inode.Name, inode)
	if err != nil {
		fuse.Debug("Error from get")
		return err
	}
	if ok == nil {
		fuse.Debug("missing metadata entry")
	}

	inode.Size = uint64(len(req.Data))

	err = fh.gfs.Client.Put(METADATA_REGION, fh.inode.Name, inode)
	if err != nil {
		fuse.Debug("Error from put")
	}

	return err
}

var _ fs.HandleReadDirAller = (*FileHandle)(nil)

func (fh *FileHandle) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	fuse.Debug("---->>>> calling FileHandle.ReadDirAll")
	q := fh.gfs.Client.Query(fmt.Sprintf("select * from /metadata where parent = '%s'", fh.inode.Parent))
	q.Reference = &INode{}

	inodes, err := fh.gfs.Client.QueryForListResult(q)
	if err != nil {
		return nil, err
	}

	dirents := make([]fuse.Dirent, 0)
	for _, i := range inodes {
		node := i.(*INode)
		d := fuse.Dirent{
			Name: node.Name,
			Type: fuse.DT_File,
		}
		dirents = append(dirents, d)
	}

	return dirents, nil
}
