# delayTask

# usage 

```go
    dt:= delayTask.NewService("sample:svc", redisClient),

	dt.RegisterEventFunc(roomWorkerMessage.QEndChengFa, svr.EndChengFaHandleV1)
	dt.RegisterEventFunc(roomWorkerMessage.QEndPk, svr.EndPkHandleV1)
	
	dt.RegisterEvent(roomWorkerMessage.QEndChengFa,"",10)
```