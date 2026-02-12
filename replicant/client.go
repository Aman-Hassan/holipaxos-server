package replicant

import (
	"bufio"
	"github.com/psu-csl/replicated-store/go/holipaxos"
	pb "github.com/psu-csl/replicated-store/go/holipaxos/comm"
	logger "github.com/sirupsen/logrus"
	"net"
	"strconv"
	"strings"
)

type Client struct {
	id         int64
	reader     *bufio.Reader
	writer     *bufio.Writer
	socket     net.Conn
	multipaxos *holipaxos.Multipaxos
	manager    *ClientManager
}

func NewClient(id int64, conn net.Conn, mp *holipaxos.Multipaxos,
	manger *ClientManager) *Client {
	client := &Client{
		id:         id,
		reader:     bufio.NewReader(conn),
		writer:     bufio.NewWriter(conn),
		socket:     conn,
		multipaxos: mp,
		manager:    manger,
	}
	return client
}

func (c *Client) Parse(request string) *pb.Command {
	substrings := strings.SplitN(strings.TrimRight(request, "\n"), " ", 3)
	if len(substrings) < 2 {
		return nil
	}
	commandType := substrings[0]
	key := substrings[1]

	command := &pb.Command{Key: key}

	if commandType == "get" {
		command.Type = pb.CommandType_GET
	} else if commandType == "del" {
		command.Type = pb.CommandType_DEL
	} else if commandType == "put" {
		if len(substrings) != 3 {
			return nil
		}
		command.Type = pb.CommandType_PUT
		command.Value = substrings[2]
	} else if commandType == "add" {
		if len(substrings) != 3 {
			return nil
		}
		command.Type = pb.CommandType_ADDNODE
		command.Value = substrings[2]
	} else {
		return nil
	}
	return command
}

func (c *Client) Start() {
	go c.Read()
}

func (c *Client) Stop() {
	c.socket.Close()
}

func (c *Client) Read() {
	for {
		request, err := c.reader.ReadString('\n')
		if err != nil {
			c.manager.Stop(c.id)
			return
		}

		command := c.Parse(request)
		if command != nil {
			logger.Infof("DEBUG [client.go Read()]: Client %d sent %s", c.id, request)
			result := c.multipaxos.Replicate(command, c.id)
			if result.Type == holipaxos.Ok {
				continue
			}
			if result.Type == holipaxos.Retry {
				logger.Infof("DEBUG [client.go Read()]: Replication failed for client %d, asking client to retry command: %s", c.id, request)
				c.Write("retry")
			} else {
				if result.Type != holipaxos.SomeElseLeader {
					panic("Result is not someone_else_leader")
				}
				logger.Infof("DEBUG [client.go Read()]: Client %d sent command to non-leader, leader is %d", c.id, result.Leader)
				c.Write("leader is " + strconv.FormatInt(result.Leader, 10))
			}
		} else {
			c.Write("bad command")
		}
	}
}

func (c *Client) Write(response string) {
	_, err := c.writer.WriteString(response + "\n")
	if err == nil {
		c.writer.Flush()
	}
}
