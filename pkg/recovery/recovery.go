package recovery

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	concurrency "github.com/brown-csci1270/db/pkg/concurrency"
	db "github.com/brown-csci1270/db/pkg/db"
	"github.com/otiai10/copy"

	uuid "github.com/google/uuid"
)

// Recovery Manager.
type RecoveryManager struct {
	d       *db.Database
	tm      *concurrency.TransactionManager
	txStack map[uuid.UUID]([]Log)
	fd      *os.File
	mtx     sync.Mutex
}

// Construct a recovery manager.
func NewRecoveryManager(
	d *db.Database,
	tm *concurrency.TransactionManager,
	logName string,
) (*RecoveryManager, error) {
	fd, err := os.OpenFile(logName, os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &RecoveryManager{
		d:       d,
		tm:      tm,
		txStack: make(map[uuid.UUID][]Log),
		fd:      fd,
	}, nil
}

// Write the string `s` to the log file. Expects rm.mtx to be locked
func (rm *RecoveryManager) writeToBuffer(s string) error {
	_, err := rm.fd.WriteString(s)
	if err != nil {
		return err
	}
	err = rm.fd.Sync()
	return err
}

// Write a Table log.
func (rm *RecoveryManager) Table(tblType string, tblName string) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	tbLog := tableLog{
		tblType: tblType,
		tblName: tblName,
	}
	rm.writeToBuffer(tbLog.toString())
}

// Write an Edit log.
func (rm *RecoveryManager) Edit(clientId uuid.UUID, table db.Index, action Action, key int64, oldval int64, newval int64) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	edLog := editLog{
		id:        clientId,
		tablename: table.GetName(),
		action:    action,
		key:       key,
		oldval:    oldval,
		newval:    newval,
	}
	rm.writeToBuffer(edLog.toString())
	rm.txStack[clientId] = append(rm.txStack[clientId], &edLog)
}

// Write a transaction start log.
func (rm *RecoveryManager) Start(clientId uuid.UUID) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	stLog := startLog{
		id: clientId,
	}
	rm.writeToBuffer(stLog.toString())
	rm.txStack[clientId] = []Log{&stLog}
}

// Write a transaction commit log.
func (rm *RecoveryManager) Commit(clientId uuid.UUID) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	cmLog := commitLog{
		id: clientId,
	}
	delete(rm.txStack, clientId)
	rm.writeToBuffer(cmLog.toString())
}

// Flush all pages to disk and write a checkpoint log.
func (rm *RecoveryManager) Checkpoint() {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	tables := rm.d.GetTables()
	for _, idx := range tables {
		idx.GetPager().LockAllUpdates()
		idx.GetPager().FlushAllPages()
		idx.GetPager().UnlockAllUpdates()
	}
	ckLog := checkpointLog{}
	for id := range rm.txStack {
		ckLog.ids = append(ckLog.ids, id)
	}
	rm.writeToBuffer(ckLog.toString())
	rm.Delta() // Sorta-semi-pseudo-copy-on-write (to ensure db recoverability)
}

// Redo a given log's action.
func (rm *RecoveryManager) Redo(log Log) error {
	switch log := log.(type) {
	case *tableLog:
		payload := fmt.Sprintf("create %s table %s", log.tblType, log.tblName)
		err := db.HandleCreateTable(rm.d, payload, os.Stdout)
		if err != nil {
			return err
		}
	case *editLog:
		switch log.action {
		case INSERT_ACTION:
			payload := fmt.Sprintf("insert %v %v into %s", log.key, log.newval, log.tablename)
			err := db.HandleInsert(rm.d, payload)
			if err != nil {
				// There is already an entry, try updating
				payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.newval)
				err = db.HandleUpdate(rm.d, payload)
				if err != nil {
					return err
				}
			}
		case UPDATE_ACTION:
			payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.newval)
			err := db.HandleUpdate(rm.d, payload)
			if err != nil {
				// Entry may have been deleted, try inserting
				payload := fmt.Sprintf("insert %v %v into %s", log.key, log.newval, log.tablename)
				err := db.HandleInsert(rm.d, payload)
				if err != nil {
					return err
				}
			}
		case DELETE_ACTION:
			payload := fmt.Sprintf("delete %v from %s", log.key, log.tablename)
			err := db.HandleDelete(rm.d, payload)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("can only redo edit logs")
	}
	return nil
}

// Undo a given log's action.
func (rm *RecoveryManager) Undo(log Log) error {
	switch log := log.(type) {
	case *editLog:
		switch log.action {
		case INSERT_ACTION:
			payload := fmt.Sprintf("delete %v from %s", log.key, log.tablename)
			err := HandleDelete(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		case UPDATE_ACTION:
			payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.oldval)
			err := HandleUpdate(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		case DELETE_ACTION:
			payload := fmt.Sprintf("insert %v %v into %s", log.key, log.oldval, log.tablename)
			err := HandleInsert(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("can only undo edit logs")
	}
	return nil
}

// Do a full recovery to the most recent checkpoint on startup.
func (rm *RecoveryManager) Recover() error {
	logs, pos, err := rm.readLogs()
	if err != nil {
		return err
	}
	for _, log := range logs {
		switch log := log.(type) {
		case *tableLog:
			rm.Redo(log)
		}
	}
	pos += 1
	actives := make(map[uuid.UUID]bool)
	for pos < len(logs) {
		log := logs[pos]
		log, err := FromString(log.toString())
		if err != nil {
			pos += 1
			continue
		}
		switch log := log.(type) {
		// case *tableLog:
		// rm.Redo(log)
		case *editLog:
			actives[log.id] = true
			rm.Redo(log)
		case *startLog:
			actives[log.id] = true
			rm.tm.Begin(log.id)
			// rm.Start(log.id)
		case *commitLog:
			delete(actives, log.id)
			rm.tm.Commit(log.id)
			// rm.Commit(log.id)
		}
		pos += 1
	}
	pos = len(logs) - 1
	for pos >= 0 {
		log := logs[pos]
		log, err := FromString(log.toString())
		if err != nil {
			pos -= 1
			continue
		}
		switch log := log.(type) {
		case *editLog:
			if _, ok := actives[log.id]; ok {
				rm.Undo(log)
			}
		case *startLog:
			if _, ok := actives[log.id]; ok {
				delete(actives, log.id)
			}
		}
		pos -= 1
	}
	return nil
}

// Roll back a particular transaction.
func (rm *RecoveryManager) Rollback(clientId uuid.UUID) error {
	logs := rm.txStack[clientId]
	i := len(logs) - 1
	for i > 0 {
		log := logs[i]
		log, err := FromString(log.toString())
		if err != nil {
			i -= 1
			continue
		}
		rm.Undo(log)
		i -= 1
	}
	rm.Commit(clientId)
	return rm.tm.Commit(clientId)
}

// Primes the database for recovery
func Prime(folder string) (*db.Database, error) {
	// Ensure folder is of the form */
	base := strings.TrimSuffix(folder, "/")
	recoveryFolder := base + "-recovery/"
	dbFolder := base + "/"
	if _, err := os.Stat(dbFolder); err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(recoveryFolder, 0775)
			if err != nil {
				return nil, err
			}
			return db.Open(dbFolder)
		}
		return nil, err
	}
	if _, err := os.Stat(recoveryFolder); err != nil {
		if os.IsNotExist(err) {
			return db.Open(dbFolder)
		}
		return nil, err
	}
	os.RemoveAll(dbFolder)
	err := copy.Copy(recoveryFolder, dbFolder)
	if err != nil {
		return nil, err
	}
	return db.Open(dbFolder)
}

// Should be called at end of Checkpoint.
func (rm *RecoveryManager) Delta() error {
	folder := strings.TrimSuffix(rm.d.GetBasePath(), "/")
	recoveryFolder := folder + "-recovery/"
	folder += "/"
	os.RemoveAll(recoveryFolder)
	err := copy.Copy(folder, recoveryFolder)
	return err
}
