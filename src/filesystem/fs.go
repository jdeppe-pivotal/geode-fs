package filesystem

import (
	"os"
	"fmt"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"github.com/gemfire/geode-go-client/connector"
	geode "github.com/gemfire/geode-go-client"
		"sync"
	"path"
	"strconv"
	"github.com/gemfire/geode-go-client/query"
	"strings"
)

const METADATA_REGION = "metadata"
const BLOCKS_REGION = "blocks"

type GFS struct {
	Client   *geode.Client
	sync.RWMutex
	handleId uint64
}

func NewGfs(serverAndPort string) *GFS {
	parts := strings.Split(serverAndPort, ":")
	port, _ := strconv.Atoi(parts[1])

	p := connector.NewPool()
	p.AddServer(parts[0], port)
	conn := connector.NewConnector(p)
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
	i := &INode{
		Name: "/",
	}

	n := &Dir{
		gfs:  g,
		inode: i,
	}
	return n, nil
}

type Dir struct {
	gfs   *GFS
	inode *INode
}

var _ fs.Node = (*Dir)(nil)

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0755
	return nil
}

var _ fs.NodeStringLookuper = (*Dir)(nil)

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	v := &INode{}
	fullPath := path.Join(d.inode.Parent, d.inode.Name, name)
	ok, err := d.gfs.Client.Get(METADATA_REGION, fullPath, v)
	fuse.Debug(fmt.Sprintf("--->>> get ok: %+v fullPath: %s", ok, fullPath))
	fuse.Debug(fmt.Sprintf("--->>> get name: %+v", name))
	fuse.Debug(fmt.Sprintf("--->>> get parent: %+v", d.inode.Name))
	fuse.Debug(fmt.Sprintf("--->>> get value: %+v", v))
	fuse.Debug(fmt.Sprintf("--->>> get err: %v", err))
	if err != nil {
		return nil, err
	}

	if ok == nil {
		return nil, fuse.ENOENT
	}

	var node fs.Node
	if (v.IsDirectory) {
		node = &Dir{
			inode: v,
			gfs:   d.gfs,
		}
	} else {
		node = &File{
			inode: v,
			gfs:   d.gfs,
		}
	}

	return node, nil
}

var _ fs.HandleReadDirAller = (*Dir)(nil)

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	fuse.Debug("---->>>> calling Dir.ReadDirAll")
	q := query.NewQuery(fmt.Sprintf("select * from /metadata where parent = '%s'", path.Join(d.inode.Parent, d.inode.Name)))
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
	fuse.Debug(fmt.Sprintf("---->>>> Dir.ReadDirAll returning %d entries\n", len(dirents)))

	return dirents, nil
}

var _ fs.NodeMkdirer = (*Dir)(nil)

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	v := &INode{
		Name:        req.Name,
		Parent:      path.Join(d.inode.Parent, d.inode.Name),
		Mode:        req.Mode,
		Id:          uint64(d.gfs.getNewHandleId()),
		IsDirectory: true,
	}
	fullPath := path.Join(d.inode.Parent, d.inode.Name, req.Name)
	err := d.gfs.Client.Put(METADATA_REGION, fullPath, v)
	if err != nil {
		return nil, err
	}

	node := &Dir{
		gfs:   d.gfs,
		inode: v,
	}

	fuse.Debug(fmt.Sprintf("--->>> Created dir %+v node:%+v", node.inode, node))
	return node, nil
}

//var _ = fs.HandleReadDirAller(&Dir{})
//
//func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
//	fuse.Debug("Calling ReadDirAll")
//	return nil, fuse.ENOENT
//}

type File struct {
	inode *INode
	gfs   *GFS
}

var _ fs.Node = (*File)(nil)

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	//zipAttr(f.file, a)
	a.Size = f.inode.Size
	a.Mode = f.inode.Mode
	a.Inode = f.inode.Id

	return nil
}

var _ fs.NodeCreater = (*Dir)(nil)

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	return createNode(ctx, req, resp, d.gfs, path.Join(d.inode.Parent, d.inode.Name))
}

//var _ fs.NodeCreater = (*File)(nil)
//
//func (f *File) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
//	return createNode(ctx, req, resp, f.gfs, f.inode.Name)
//}

func createNode(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse, gfs *GFS, parent string) (fs.Node, fs.Handle, error) {
	resp.Handle = gfs.getNewHandleId()

	v := &INode{
		Name:   req.Name,
		Parent: parent,
		Mode:   req.Mode,
		Id:     uint64(resp.Handle),
	}
	fullPath := path.Join(parent, req.Name)
	err := gfs.Client.Put(METADATA_REGION, fullPath, v)
	if err != nil {
		return nil, nil, err
	}

	node := &File{
		inode: v,
		gfs:   gfs,
	}
	handle := &FileHandle{
		inode: v,
		gfs:   gfs,
	}

	fuse.Debug(fmt.Sprintf("--->>> Created inode %+v node:%+v", node.inode, node))

	return node, handle, nil
}

var _ = fs.NodeOpener(&File{})

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	handle := &FileHandle{
		inode: f.inode,
		gfs:   f.gfs,
	}

	resp.Flags |= fuse.OpenNonSeekable
	resp.Handle = f.gfs.getNewHandleId()

	fuse.Debug(fmt.Sprintf("--->>> Open: %+v", f.inode))

	return handle, nil
}

var _ fs.NodeGetattrer = (*File)(nil)

func (f *File) Getattr(ctx context.Context, req *fuse.GetattrRequest, resp *fuse.GetattrResponse) error {
	resp.Attr.Inode = f.inode.Id
	resp.Attr.Size = f.inode.Size
	resp.Attr.Mode = f.inode.Mode
	fuse.Debug(fmt.Sprintf("--->>> Getattr: %+v f:%+v", f.inode, f))

	return nil
}

var _ fs.NodeSetattrer = (*File)(nil)

func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	if req.Valid.Mode() {
		f.inode.Mode = req.Mode
	}

	if req.Valid.Size() {
		f.inode.Size = req.Size
	}

	fullPath := path.Join(f.inode.Parent, f.inode.Name)
	err := f.gfs.Client.Put(METADATA_REGION, fullPath, f.inode)
	if err != nil {
		return err
	}

	fuse.Debug(fmt.Sprintf("--->>> Setattr: %+v f:%+v", f.inode, f))

	return nil
}

var _ fs.NodeRemover = (*Dir)(nil)

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	fullPath := path.Join(d.inode.Parent, d.inode.Name)
	return d.gfs.Client.Remove(METADATA_REGION, fullPath)
}

type FileHandle struct {
	//r     io.ReadCloser
	inode *INode
	gfs   *GFS
}

var _ fs.Handle = (*FileHandle)(nil)

//var _ fs.HandleReleaser = (*FileHandle)(nil)

//func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
//	return fh.r.Close()
//}

var _ fs.HandleReadAller = (*FileHandle)(nil)

func (fh *FileHandle) ReadAll(ctx context.Context) ([]byte, error) {
	fuse.Debug("--->>> ReadAll")
	fullPath := path.Join(fh.inode.Parent, fh.inode.Name)
	rawData, err := fh.gfs.Client.Get(BLOCKS_REGION, fullPath)
	if err != nil {
		return nil, err
	}

	fuse.Debug(fmt.Sprintf("--->>> ReadAll: %+v", rawData))

	return rawData.([]byte), nil
}

var _ fs.HandleReader = (*FileHandle)(nil)

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	fullPath := path.Join(fh.inode.Parent, fh.inode.Name)
	rawData, err := fh.gfs.Client.Get(BLOCKS_REGION, fullPath)
	if err != nil {
		return err
	}

	fuse.Debug(fmt.Sprintf("--->>> Read: %+v", rawData))
	resp.Data = rawData.([]byte)

	return nil
}

var _ fs.HandleWriter = (*FileHandle)(nil)

func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	fuse.Debug(fmt.Sprintf("--->>> writing to %s fh:%+v", fh.inode.Name, fh))
	fullPath := path.Join(fh.inode.Parent, fh.inode.Name)
	err := fh.gfs.Client.Put(BLOCKS_REGION, fullPath, req.Data)
	resp.Size = len(req.Data)

	inode := &INode{}
	ok, err := fh.gfs.Client.Get(METADATA_REGION, fullPath, inode)
	if err != nil {
		fuse.Debug("Error from get")
		return err
	}
	if ok == nil {
		fuse.Debug("missing metadata entry")
	}

	inode.Size = uint64(len(req.Data))

	err = fh.gfs.Client.Put(METADATA_REGION, fullPath, inode)
	if err != nil {
		fuse.Debug("Error from put")
	}
	*fh.inode = *inode

	return err
}

var _ fs.HandleReadDirAller = (*FileHandle)(nil)

func (fh *FileHandle) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	fuse.Debug("---->>>> calling FileHandle.ReadDirAll")
	q := query.NewQuery(fmt.Sprintf("select * from /metadata where parent = '%s'", fh.inode.Parent))
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
