package connect

func Initialization(serviceName string) {
	_ = ConnectLog(serviceName)
	_ = ConnectStdLog(serviceName)
	InitJaeger(serviceName)
	MysqlInit(serviceName)
}
