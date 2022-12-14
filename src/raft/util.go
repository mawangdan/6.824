package raft

import (
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
)

// Debugging
const Debug = true

var logMu sync.Mutex

type LogType uint64

const (
	LogAll         LogType = 0xffffffffffffffff
	LogAESend      LogType = 1
	LogAERev       LogType = 1 << 1
	LogRVSend      LogType = 1 << 2
	LogRVRev       LogType = 1 << 3
	LogElec        LogType = 1 << 4
	LogHeartBeat   LogType = 1 << 5
	LogStateChange LogType = 1 << 6
	LogRVBody      LogType = 1 << 7
	LogRP          LogType = 1 << 8
	LogAEBody      LogType = 1 << 9
	LogApply       LogType = 1 << 10
)

const (
	LogEAH LogType = LogRVRev | LogRVSend | LogAESend | LogAERev | LogRVBody | LogAEBody | LogRP | LogApply
)

const LogSwitch LogType = LogEAH

var LStoStr = map[LogType]string{
	LogAESend:      "LogAESend",
	LogAERev:       "LogAERev",
	LogRVSend:      "LogRVSend",
	LogRVRev:       "LogRVRev",
	LogElec:        "LogElec",
	LogHeartBeat:   "LogHeartBeat",
	LogStateChange: "LogStateChange",
	LogAll:         "LogAll",
	LogRVBody:      "LogRVBody",
	LogRP:          "LogRP",
	LogAEBody:      "LogAEBody",
	LogApply:       "LogApply",
}

// 前景 背景 颜色
// ---------------------------------------
// 30  40  黑色
// 31  41  红色
// 32  42  绿色
// 33  43  黄色
// 34  44  蓝色
// 35  45  紫红色
// 36  46  青蓝色
// 37  47  白色
//
// 代码 意义
// -------------------------
//  0  终端默认设置
//  1  高亮显示
//  4  使用下划线
//  5  闪烁
//  7  反白显示
//  8  不可见
type Color int

const (
	Black     Color = 30
	Red       Color = 31
	Green     Color = 32
	Yellow    Color = 33
	Blue      Color = 34
	Purple    Color = 35
	GreenBlue Color = 36
	White     Color = 37
)

//First arg  Color: Black  Red  Green   Yellow  Blue   Purple   GreenBlue  White
//Second arg  msg
func SetColor(c Color, msg string) string {
	// conf := 0    // 配置、终端默认设置
	// bg := 0      // 背景色、终端默认设置
	// var text int // 前景色
	// text = int(c)
	//return fmt.Sprintf("%c[%d;%d;%dm%s%c[0m", 0x1B, conf, bg, text, msg, 0x1B)
	return msg
}

func (ls LogType) String() string {
	return LStoStr[ls]
}
func DPrintf(lt LogType, perfix string, format string, a ...interface{}) (n int, err error) {
	if Debug {
		if lt&LogSwitch != 0 {

			//哈哈!从log的源码复制过来的
			_, file, line, ok := runtime.Caller(2)
			if !ok {
				file = "???"
				line = 0
			}
			short := file
			for i := len(file) - 1; i > 0; i-- {
				if file[i] == '/' {
					short = file[i+1:]
					break
				}
			}
			file = short

			perfix += file + ":" + strconv.Itoa(line)
			logMu.Lock()
			log.SetPrefix("[" + perfix + "]")
			log.Printf(format, a...)
			logMu.Unlock()
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

var rpcn = 1
var rpcnMu sync.Mutex

func getRpcn() int {
	rpcnMu.Lock()
	ret := rpcn
	rpcn++
	rpcnMu.Unlock()
	return ret
}
