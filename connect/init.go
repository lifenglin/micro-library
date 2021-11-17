package connect

import "os"

func Initialization(serviceName string) {
	_ = ConnectLog(serviceName)
	_ = ConnectStdLog(serviceName)
	if isProduction() {
		//InitJaeger(serviceName)
		MysqlInit(serviceName)
	}
}

func isProduction() bool {
	if os.Getenv("POD_NAMESPACE") == "production" {
		return true
	} else {
		return false
	}
}
