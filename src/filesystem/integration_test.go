package filesystem_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"syscall"
	"log"
	"path"
	"os"
	"net"
	geode "github.com/gemfire/geode-go-client"
	"github.com/gemfire/geode-go-client/connector"
	"filesystem"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

var _ = Describe("Sanity", func() {

	var tempMountPoint string

	BeforeSuite(func() {
		fuse.Debug = func(msg interface{}) {
			log.Print(msg)
		}

		var err error
		tempMountPoint, err = ioutil.TempDir("", "gfs")
		if err != nil {
			panic(err)
		}

		c, err := fuse.Mount(tempMountPoint, fuse.NoAppleDouble(), fuse.NoAppleXattr())
		if err != nil {
			panic(err)
		}

		connection, err := net.Dial("tcp4", "localhost:40404")
		Expect(err).To(BeNil())

		p := connector.NewPool(connection)
		conn := connector.NewConnector(p)
		client := geode.NewGeodeClient(conn)
		err = client.Connect()
		Expect(err).To(BeNil())

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

	Context("when creating a single file", func() {
		It("it can be read back", func() {
			content := "foo"
			file := path.Join(tempMountPoint, "foo")
			err := ioutil.WriteFile(file, []byte(content), os.ModePerm)
			Expect(err).To(BeNil())

			result, err := ioutil.ReadFile(file)
			Expect(err).To(BeNil())
			Expect(string(result)).To(Equal(content))
		})
	})
})

