package frontend

import (
	"time"
	"sync"
	rclient "router/client"
	"router/router"
	"storage"
)

// InitTimeout is a timeout to wait after unsuccessful List() request to Router.
//
// InitTimeout -- количество времени, которое нужно подождать до следующей попытки
// отправки запроса List() в Router.
const InitTimeout = 100 * time.Millisecond

// Config stores configuration for a Frontend service.
//
// Config -- содержит конфигурацию Frontend.
type Config struct {
	// Addr is an address to listen at.
	// Addr -- слушающий адрес Frontend.
	Addr storage.ServiceAddr
	// Router is an address of Router service.
	// Router -- адрес Router service.
	Router storage.ServiceAddr

	// NC specifies client for Node.
	// NC -- клиент для node.
	NC storage.Client `yaml:"-"`
	// RC specifies client for Router.
	// RC -- клиент для router.
	RC rclient.Client `yaml:"-"`
	// NodesFinder specifies a NodeFinder to use.
	// NodesFinder -- NodesFinder, который нужно использовать в Frontend.
	NF router.NodesFinder `yaml:"-"`
}

// Frontend is a frontend service.
type Frontend struct {
	cfg Config
	nodesList []storage.ServiceAddr
	initControll sync.Once
}

// New creates a new Frontend with a given cfg.
//
// New создает новый Frontend с данным cfg.
func New(cfg Config) *Frontend {
	return &Frontend{cfg: cfg}
}


func (fe *Frontend) updateWork(k storage.RecordID, task func(node storage.ServiceAddr) error) error {
	nodes, err := fe.cfg.RC.NodesFind(fe.cfg.Router, k)
	if err != nil {
		return err
	}
	if len(nodes) < storage.MinRedundancy {
		return storage.ErrNotEnoughDaemons
	}

	errors := make(map[error]int)
	errorsChanel := make(chan error, len(nodes))

	for _, node := range nodes {
		go func(node storage.ServiceAddr) {
			errorsChanel <- task(node)
		}(node)
	}

	for range nodes {
		errors[<- errorsChanel]++
	}

	for err, num := range errors {
		if num >= storage.MinRedundancy {
			return err
		}
	}

	return storage.ErrQuorumNotReached
}

// Put an item to the storage if an item for the given key doesn't exist.
// Returns error otherwise.
//
// Put -- добавить запись в хранилище, если запись для данного ключа
// не существует. Иначе вернуть ошибку.
func (fe *Frontend) Put(k storage.RecordID, d []byte) error {
	return fe.updateWork(k, func(node storage.ServiceAddr) error {
		return fe.cfg.NC.Put(node, k, d)
	})
}

// Del an item from the storage if an item exists for the given key.
// Returns error otherwise.
//
// Del -- удалить запись из хранилища, если запись для данного ключа
// существует. Иначе вернуть ошибку.
func (fe *Frontend) Del(k storage.RecordID) error {
	return fe.updateWork(k, func(node storage.ServiceAddr) error {
		return fe.cfg.NC.Del(node, k)
	})
}

// Get an item from the storage if an item exists for the given key.
// Returns error otherwise.
//
// Get -- получить запись из хранилища, если запись для данного ключа
// существует. Иначе вернуть ошибку.
func (fe *Frontend) Get(k storage.RecordID) ([]byte, error) {
	fe.initControll.Do(func() {
		for {
			var err error
			fe.nodesList, err = fe.cfg.RC.List(fe.cfg.Router)
			if err == nil {
				break
			}
			time.Sleep(InitTimeout)
		}
	})

	nodes := fe.cfg.NF.NodesFind(k, fe.nodesList)
	if len(nodes) < storage.MinRedundancy {
		return nil, storage.ErrNotEnoughDaemons
	}

	type result struct {
		data []byte
		retError error
	}

	errors := make(map[error]int)
	results := make(map[string]int)
	resultsChanel := make(chan result, len(nodes))

	for _,node := range nodes {
		go func(node storage.ServiceAddr) {
			data, err := fe.cfg.NC.Get(node, k)
			resultsChanel <- result{data, err}
		}(node)
	}

	for range nodes {
		res := <- resultsChanel
		if res.retError == nil {
			results[string(res.data)]++
			if results[string(res.data)] == storage.MinRedundancy {
				return res.data, nil
			}
		} else {
			errors[res.retError]++
			if errors[res.retError] == storage.MinRedundancy {
				return nil, res.retError
			}
		}
	}

	return nil, storage.ErrQuorumNotReached

}
