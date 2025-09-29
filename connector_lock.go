package icanal

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
)

const consumerPath = "/canal-consumer"

func getLockPath(destination string) string {
	return fmt.Sprintf("%s/%s", consumerPath, destination)
}

func checkAndCreateEphemeralSequence(zkConn *zk.Conn, lockPath string, sequence string) (string, error) {
	children, _, err := zkConn.Children(lockPath)
	if err != nil {
		return "", err
	}
	// 查看是否创建临时节点
	if !contains(children, sequence) {
		path, err := createEphemeralSequence(zkConn, lockPath)
		if err != nil {
			return "", err
		}
		return path, nil
	}
	return "", nil
}

func checkRootPath(zkConn *zk.Conn, rootPath string) error {
	parts := strings.Split(rootPath, "/")

	for i := 1; i < len(parts); i++ {
		path := strings.Join(parts[:i+1], "/")
		exists, _, err := zkConn.Exists(path)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		if _, err = zkConn.Create(path, []byte{}, 0, zk.WorldACL(zk.PermAll)); err != nil {
			return err
		}
	}

	return nil
}

const (
	notRunningFlag = byte(0)
)

func createEphemeralSequence(zkConn *zk.Conn, path string) (string, error) {
	node, err := zkConn.Create(path+"/",
		[]byte{notRunningFlag},
		zk.FlagEphemeral|zk.FlagSequence,
		zk.WorldACL(zk.PermAll))
	if err != nil {
		return "", err
	}
	parts := strings.Split(node, "/")
	return parts[len(parts)-1], nil
}

// 等待删除
func waitDelete(zkConn *zk.Conn, previousPath string) error {
	exists, _, events, err := zkConn.ExistsW(previousPath)
	if err != nil {
		return err
	}

	if exists {
		event := <-events
		if event.Type != zk.EventNodeDeleted {
			return waitDelete(zkConn, previousPath)
		} else {
			time.Sleep(10 * time.Second)
			return waitDelete(zkConn, previousPath)
		}
	}

	return nil
}
