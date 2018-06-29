package filesystem

type INode struct {
	Name        string `json:"name"`
	Parent      string `json:"parent"`
	Size        uint64 `json:"size"`
	IsDirectory bool   `json:"isDirectory"`
}
