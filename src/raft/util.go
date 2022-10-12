package raft

import (
	"log"
	"os"
	"strconv"
)

// Debugging
const Debug = true

type LogType uint64

const (
	LogAESend      LogType = 1
	LogAERev       LogType = 1 << 1
	LogRVSend      LogType = 1 << 2
	LogRVRev       LogType = 1 << 3
	LogElec        LogType = 1 << 4
	LogHeartBeat   LogType = 1 << 5
	LogStateChange LogType = 1 << 6
)

const LogFlag = LogAESend | LogAERev | LogRVSend | LogRVRev | LogElec | LogHeartBeat | LogStateChange

var LStoStr = map[LogType]string{
	LogAESend:      "LogAESend",
	LogAERev:       "LogAERev",
	LogRVSend:      "LogRVSend",
	LogRVRev:       "LogRVRev",
	LogElec:        "LogElec",
	LogHeartBeat:   "LogHeartBeat",
	LogStateChange: "LogStateChange",
}

func (ls LogType) String() string {
	return LStoStr[ls]
}
func DPrintf(lt LogType, perfix string, format string, a ...interface{}) (n int, err error) {
	if Debug {
		if lt&LogFlag != 0 {
			log.SetPrefix("[" + perfix + "]")
			log.Printf(format, a...)
		}
	}
	return
}

func initLog(file string) {
	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile) // 将文件设置为log输出的文件
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.LUTC)
	return
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func intTo64(i int) int64 {
	str := strconv.Itoa(i)
	i64, _ := strconv.ParseInt(str, 10, 64)
	return i64
}

func i64Toint(i64 int64) int {
	str := strconv.FormatInt(i64, 10)
	i, _ := strconv.Atoi(str)
	return i
}
