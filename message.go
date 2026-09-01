package Redelay

type Message[T any] struct {
	//OpId string `json:"opId" required:"true"`
	MsgId string `json:"msgId" required:"true"`
	Data  T      `json:"data"`
}
