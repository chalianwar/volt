package zookeeper

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/VoltFramework/volt/task"
	"github.com/samuel/go-zookeeper/zk"
)

var (
	ErrNotExists = errors.New("task does not exist")
	flags        = int32(0)
	acl          = zk.WorldACL(zk.PermAll)
)

type Registry struct {
	conn *zk.Conn
	path string
}

func New(uris string) *Registry {
	var (
		parts     = strings.SplitN(uris[5:], "/", 2)
		c, _, err = zk.Connect(strings.Split(parts[0], ","), time.Second)
		path      = "/" + parts[1]
	)

	if err != nil {
		return nil
	}

	if exists, _, _ := c.Exists(path); !exists {
		c.Create(path, []byte("volt"), flags, acl)
		c.Create(path+"/tasks", []byte("tasks"), flags, acl)
	}
	return &Registry{
		conn: c,
		path: path,
	}
}

func (r *Registry) Register(id string, task *task.Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	_, err = r.conn.Create(r.path+"/tasks/"+id, data, flags, acl)
	return err
}

func (r *Registry) Fetch(id string) (*task.Task, error) {
	if exists, _, _ := r.conn.Exists(r.path + "/tasks/" + id); !exists {
		return nil, ErrNotExists
	}

	data, _, err := r.conn.Get(r.path + "/tasks/" + id)
	if err != nil {
		return nil, err
	}

	t := new(task.Task)
	err = json.Unmarshal(data, t)
	return t, err
}

func (r *Registry) Tasks() ([]*task.Task, error) {
	var out []*task.Task

	children, _, err := r.conn.Children(r.path + "/tasks")
	if err != nil {
		return nil, err
	}

	for _, v := range children {
		t, err := r.Fetch(v)
		if err == nil {
			out = append(out, t)
		}
	}

	return out, nil
}

func (r *Registry) Update(id string, t *task.Task) error {
	_, stat, err := r.conn.Get(r.path + "/tasks/" + id)
	if err != nil {
		return err
	}

	data, err := json.Marshal(t)
	if err != nil {
		return err
	}
	_, err = r.conn.Set(r.path+"/tasks/"+id, data, stat.Version)

	return err
}

func (r *Registry) Delete(id string) error {
	return r.conn.Delete(r.path+"/tasks/"+id, 0)
}