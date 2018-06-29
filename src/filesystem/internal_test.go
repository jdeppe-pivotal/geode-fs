package filesystem_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"syscall"
	"github.com/gemfire/geode-go-client/connector/connectorfakes"
	"github.com/gemfire/geode-go-client/connector"
	v1 "github.com/gemfire/geode-go-client/protobuf/v1"
	geode "github.com/gemfire/geode-go-client"
	"filesystem"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"log"
	"github.com/golang/protobuf/proto"
	"os"
	"path"
	"fmt"
)

var _ = Describe("Sanity", func() {

	var tempMountPoint string

	var fakeConn *connectorfakes.FakeConn

	BeforeSuite(func() {
		fuse.Debug = func(msg interface{}) {
			log.Print(msg)
		}

		var err error
		tempMountPoint, err = ioutil.TempDir("", "gfs")
		if err != nil {
			panic(err)
		}

		c, err := fuse.Mount(tempMountPoint, fuse.NoAppleDouble())
		if err != nil {
			panic(err)
		}

		fakeConn = new(connectorfakes.FakeConn)
		p := connector.NewPool(fakeConn)
		conn := connector.NewConnector(p)
		client := geode.NewGeodeClient(conn)

		gfsHandle := &filesystem.GFS{Client: client}

		go func() {
			if err := fs.Serve(c, gfsHandle); err != nil {
				panic(err)
			}

			//check if the mount process has an error to report
			<-c.Ready
			if err := c.MountError; err != nil {
				panic(err)
			}
		}()

	})

	AfterSuite(func() {
		fuse.Unmount(tempMountPoint)
		syscall.Unlink(tempMountPoint)
	})

	Context("When calling os.Stat on a file", func() {
		It("can be queried for attributes", func() {
			var v *v1.EncodedValue
			inode := &filesystem.INode{
				Name: "foo",
				Size: 100,
				IsDirectory: false,
			}
			v, _ = connector.EncodeValue(inode)

			fakeConn.ReadStub = func(b []byte) (int, error) {
				response := &v1.Message{
					MessageType: &v1.Message_GetResponse{
						GetResponse: &v1.GetResponse{
							Result: v,
						},
					},
				}
				return writeFakeMessage(response, b)
			}

			info, err := os.Stat(path.Join(tempMountPoint, "foo"))

			Expect(err).To(BeNil())
			Expect(info.Name()).To(Equal("foo"))
			Expect(info.Size()).To(Equal(int64(100)))
			Expect(info.IsDir()).To(BeFalse())
		})
	})

	Context("When creating and writing to a file", func() {
		FIt("Does not produce any errors", func() {
			var v *v1.EncodedValue
			//inode := &filesystem.INode{
			//	Name: "test2",
			//	Size: 100,
			//	IsDirectory: false,
			//}
			v, _ = connector.EncodeValue(nil)

			fakeConn.ReadStub = func(b []byte) (int, error) {
				response := &v1.Message{
					MessageType: &v1.Message_GetResponse{
						GetResponse: &v1.GetResponse{
							Result: v,
						},
					},
				}
				return writeFakeMessage(response, b)
			}

			fakeConn.WriteStub = func(b []byte) (int, error) {
				fmt.Printf("------>>> calling fake write - len %d\n", len(b))
				return len(b), nil
			}

			file, err := os.Create(path.Join(tempMountPoint, "test2"))
			Expect(err).To(BeNil())

			n, err := file.WriteString("Test")
			Expect(n).To(Equal(4))
		})
	})
})

func writeFakeMessage(m proto.Message, b []byte) (int, error) {
	p := proto.NewBuffer(nil)
	p.EncodeMessage(m)
	n := copy(b, p.Bytes())

	return n, nil
}
