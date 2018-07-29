package filesystem_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"syscall"
	"log"
	"path"
	"os"
		geode "github.com/gemfire/geode-go-client"
	"github.com/gemfire/geode-go-client/connector"
	"filesystem"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"fmt"
	"os/exec"
)

func gfsh(command string) {
	var connectCmd string
	connectCmd = fmt.Sprintf("connect --locator=localhost[%d]", 10334)

	args := append([]string{"-e", connectCmd, "-e", command})

	gfsh := exec.Command(os.ExpandEnv("$GEODE_HOME/bin/gfsh"), args...)

	gfsh.Stdout = os.Stdout
	gfsh.Stderr = os.Stderr

	Expect(gfsh.Run()).To(BeNil())
}

func resetRegions() {
	gfsh("destroy region --name=/metadata --if-exists")
	gfsh("destroy region --name=/blocks --if-exists")
	gfsh("create region --name=/metadata --type=PARTITION")
	gfsh("create region --name=/blocks --type=PARTITION")
}

var _ = Describe("Sanity", func() {
	var tempMountPoint string
	var client *geode.Client

	BeforeSuite(func() {
		var err error

		resetRegions()

		fuse.Debug = func(msg interface{}) {
			log.Print(msg)
		}

		tempMountPoint, err = ioutil.TempDir("", "gfs")
		if err != nil {
			panic(err)
		}

		c, err := fuse.Mount(tempMountPoint, fuse.NoAppleDouble(), fuse.NoAppleXattr())
		if err != nil {
			panic(err)
		}

		p := connector.NewPool()
		p.AddServer("localhost", 40404)
		conn := connector.NewConnector(p)
		client = geode.NewGeodeClient(conn)

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

	BeforeEach(func() {
	})

	Context("when creating a single file", func() {
		It("it can be read back", func() {
			contentAndName := "test1"
			file := path.Join(tempMountPoint, contentAndName)
			err := ioutil.WriteFile(file, []byte(contentAndName), os.ModePerm)
			Expect(err).To(BeNil())

			result, err := ioutil.ReadFile(file)
			Expect(err).To(BeNil())
			Expect(string(result)).To(Equal(contentAndName))

			r := &filesystem.INode{}
			ok, err := client.Get(filesystem.METADATA_REGION, "/test1", r)
			Expect(ok).ToNot(BeNil())
			Expect(r.Name).To(Equal("test1"))
			Expect(r.Parent).To(Equal("/"))
			Expect(r.IsDirectory).To(BeFalse())
			Expect(r.Size).To(BeEquivalentTo(5))
		})

		It("it can be updated after initial creation", func() {
			contentAndName := "test2"
			file := path.Join(tempMountPoint, contentAndName)
			err := ioutil.WriteFile(file, []byte(contentAndName), os.ModePerm)
			Expect(err).To(BeNil())

			result, err := ioutil.ReadFile(file)
			Expect(err).To(BeNil())
			Expect(string(result)).To(Equal(contentAndName))

			content := "test2 updated"
			err = ioutil.WriteFile(file, []byte(content), os.ModePerm)
			Expect(err).To(BeNil())

			result, err = ioutil.ReadFile(file)
			Expect(err).To(BeNil())
			Expect(string(result)).To(Equal(content))

			r := &filesystem.INode{}
			ok, err := client.Get(filesystem.METADATA_REGION, "/test2", r)
			Expect(ok).ToNot(BeNil())
			Expect(r.Name).To(Equal("test2"))
			Expect(r.Parent).To(Equal("/"))
			Expect(r.IsDirectory).To(BeFalse())
			Expect(r.Size).To(BeEquivalentTo(13))
		})

		It("it has the correct size after being updated", func() {
			contentAndName := "test3"
			file := path.Join(tempMountPoint, contentAndName)

			f, err := os.Create(file)
			Expect(err).To(BeNil())
			f.Close()

			fileInfo, err := os.Stat(file)
			Expect(err).To(BeNil())

			Expect(fileInfo.Size()).To(BeEquivalentTo(0))

			err = ioutil.WriteFile(file, []byte(contentAndName), os.ModePerm)

			fileInfo, err = os.Stat(file)
			Expect(err).To(BeNil())
			Expect(fileInfo.Size()).To(BeEquivalentTo(len(contentAndName)))

			updatedContent := "test3 updated"
			err = ioutil.WriteFile(file, []byte(updatedContent), os.ModePerm)
			Expect(err).To(BeNil())

			fileInfo, err = os.Stat(file)
			Expect(err).To(BeNil())
			Expect(fileInfo.Size()).To(BeEquivalentTo(len(updatedContent)))

			r := &filesystem.INode{}
			ok, err := client.Get(filesystem.METADATA_REGION, "/test3", r)
			Expect(ok).ToNot(BeNil())
			Expect(r.Name).To(Equal("test3"))
			Expect(r.Parent).To(Equal("/"))
			Expect(r.IsDirectory).To(BeFalse())
			Expect(r.Size).To(BeEquivalentTo(13))
		})

		It("it has the correct mode after being created", func() {
			contentAndName := "test4"
			file := path.Join(tempMountPoint, contentAndName)

			f, err := os.OpenFile(file, os.O_CREATE | os.O_TRUNC, 0600)
			Expect(err).To(BeNil())
			f.Close()

			fileInfo, err := os.Stat(file)
			Expect(err).To(BeNil())

			Expect(fileInfo.Mode()).To(BeEquivalentTo(0600))

			r := &filesystem.INode{}
			ok, err := client.Get(filesystem.METADATA_REGION, "/test4", r)
			Expect(ok).ToNot(BeNil())
			Expect(r.Mode).To(BeEquivalentTo(0600))
		})

		It("the mode can be updated after creation", func() {
			contentAndName := "test5"
			file := path.Join(tempMountPoint, contentAndName)

			f, err := os.OpenFile(file, os.O_CREATE | os.O_TRUNC, 0600)
			Expect(err).To(BeNil())
			f.Close()

			fileInfo, err := os.Stat(file)
			Expect(err).To(BeNil())

			Expect(fileInfo.Mode()).To(BeEquivalentTo(0600))

			os.Chmod(file, 0777)

			fileInfo, err = os.Stat(file)
			Expect(err).To(BeNil())

			Expect(fileInfo.Mode()).To(BeEquivalentTo(0777))

			r := &filesystem.INode{}
			ok, err := client.Get(filesystem.METADATA_REGION, "/test5", r)
			Expect(ok).ToNot(BeNil())
			Expect(r.Mode).To(BeEquivalentTo(0777))
		})

		XIt("directories can be created", func() {
			dirname := "test6"
			directory := path.Join(tempMountPoint, dirname)

			err := os.Mkdir(directory, 0600)
			Expect(err).To(BeNil())

			fileInfo, err := os.Stat(directory)
			Expect(err).To(BeNil())

			Expect(fileInfo.IsDir()).To(Equal(true))
			Expect(fileInfo.Mode()).To(BeEquivalentTo(0600))

			r := &filesystem.INode{}
			ok, err := client.Get(filesystem.METADATA_REGION, "/test6", r)
			Expect(ok).ToNot(BeNil())
			Expect(r.IsDirectory).To(BeTrue())
		})

		It("directories can be created and populated", func() {
			dirname := "test7"
			directory := path.Join(tempMountPoint, dirname)

			err := os.Mkdir(directory, 0666)
			Expect(err).To(BeNil())

			contentAndName := "test8"
			file := path.Join(directory, contentAndName)
			err = ioutil.WriteFile(file, []byte(contentAndName), os.ModePerm)
			Expect(err).To(BeNil())

			_, err = os.Stat(file)
			Expect(err).To(BeNil())

			result, err := ioutil.ReadFile(file)
			Expect(err).To(BeNil())
			Expect(string(result)).To(Equal(contentAndName))

			files, err := ioutil.ReadDir(directory)
			Expect(err).To(BeNil())

			var names []string
			for _, f := range files {
				names = append(names, f.Name())
			}

			Expect(names).To(ContainElement(contentAndName))

			r := &filesystem.INode{}
			ok, err := client.Get(filesystem.METADATA_REGION, "/test7", r)
			Expect(ok).ToNot(BeNil())
			Expect(r.IsDirectory).To(BeTrue())

			r = &filesystem.INode{}
			ok, err = client.Get(filesystem.METADATA_REGION, "/test7/test8", r)
			Expect(ok).ToNot(BeNil())
			Expect(r.Name).To(Equal("test8"))
			Expect(r.Parent).To(Equal("/test7"))
			Expect(r.IsDirectory).To(BeFalse())
		})

		It("files with the same name can be created in different directories", func() {
			var err error
			dirname := "test9"
			directory := path.Join(tempMountPoint, dirname)

			err = os.Mkdir(directory, 0666)
			Expect(err).To(BeNil())

			commonName := "test10"
			content1:= "test10 content1"
			file1 := path.Join(tempMountPoint, commonName)
			err = ioutil.WriteFile(file1, []byte(content1), os.ModePerm)
			Expect(err).To(BeNil())

			content2 := "test10 content2"
			file2 := path.Join(tempMountPoint, dirname, commonName)
			err = ioutil.WriteFile(file2, []byte(content2), os.ModePerm)
			Expect(err).To(BeNil())

			result1, err := ioutil.ReadFile(file1)
			Expect(err).To(BeNil())
			Expect(string(result1)).To(Equal(content1))

			result2, err := ioutil.ReadFile(file2)
			Expect(err).To(BeNil())
			Expect(string(result2)).To(Equal(content2))

			files, err := ioutil.ReadDir(directory)
			Expect(err).To(BeNil())

			var names []string
			for _, f := range files {
				names = append(names, f.Name())
			}

			Expect(names).To(ContainElement(commonName))
		})
	})
})

