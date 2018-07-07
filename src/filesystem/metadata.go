package filesystem

import "os"

type INode struct {
	Name        string `json:"name"`
	Parent      string `json:"parent"`
	Size        uint64 `json:"size"`
	IsDirectory bool   `json:"isDirectory"`
	Mode        os.FileMode `json:"mode"`
	Id          uint64      `json:"id"`
}
