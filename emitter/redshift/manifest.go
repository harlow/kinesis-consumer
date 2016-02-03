package connector

type Entry struct {
	Url       string `json:"url"`
	Mandatory bool   `json:"mandatory"`
}

type Manifest struct {
	Entries []Entry `json:"entries"`
}
