package Redelay

import "github.com/afret0/wheel/tool"

type Message[T any] struct {
	//OpId string `json:"opId" required:"true"`
	MsgId string `json:"msgId" required:"true"`
	Data  T      `json:"data"`
}

func (m *Message[T]) Marshall() string {
	if m.MsgId == "" {
		m.MsgId = tool.UUIDWithoutHyphen()
	}

	return tool.MarshalWithoutErr(m)
}
