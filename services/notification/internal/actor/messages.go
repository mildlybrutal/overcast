package actor

import "github.com/gorilla/websocket"

type MessageType int

const (
	ClientJoin MessageType = iota
	ClientLeave
	BroadcastUpdate
	StopActor
)

type Client struct {
	ID   string
	Conn *websocket.Conn
	Send chan []byte
}

type ActorMessage struct {
	Type    MessageType
	Client  *Client
	Payload []byte
}
