package command

import "encoding/json"

type Action uint32

const (
	Put Action = 1 + iota
	Delete
)

type Command struct {
	Action Action `json:"action"`
	Key    string `json:"key"`
	Value  []byte `json:"value"`
}

func Serialize(c *Command) ([]byte, error) {
	s, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func Deserialize(b []byte) (*Command, error) {
	c := &Command{}
	err := json.Unmarshal(b, c)
	if err != nil {
		return nil, err
	}
	return c, nil
}
