# AMQPX - RabbitMQ Golang 客户端

AMQPX 是一个基于 Golang 的 RabbitMQ 客户端，封装了 AMQP 0-9-1 协议，提供便捷的连接管理、消息发布与消费功能。

## 功能特性
- 简化的连接管理
- 支持 TLS 连接
- 自动重连的消费者
- 轻量级生产者接口
- 优雅关闭机制

## 安装
```sh
go get github.com/amqpx/amqpx
```

## 配置
你可以使用 `Config` 结构体进行连接配置：

```go
type Config struct {
    Host         string `json:"host"`
    Port         string `json:"port"`
    Username     string `json:"username"`
    Password     string `json:"password"`
    Vhost        string `json:"vhost"`
    TlsProtocols bool   `json:"tlsProtocols"`
}
```

### 示例配置
```go
GlobalConfig = Config{
    Host:         "localhost",
    Port:         "5672",
    Username:     "guest",
    Password:     "guest",
    Vhost:        "/",
    TlsProtocols: false,
}
```

## 使用方法

### 初始化连接
调用 `Init` 进行初始化：
```go
err := amqpx.Init()
if err != nil {
    log.Fatalf("AMQP 初始化失败: %v", err)
}
defer amqpx.Close()
```

### 消息发布
使用 `Publish` 发布消息：
```go
err := amqpx.Publish("exchange_name", "routing_key", []byte("message"))
if err != nil {
    log.Printf("消息发布失败: %v", err)
}
```

### 消息消费
创建消费者并添加消费逻辑：
```go
consumer, err := amqpx.NewAmqpxConsumer()
if err != nil {
    log.Fatalf("创建消费者失败: %v", err)
}

consumer.AddFunc("queue_name", "consumer_tag", func(msg []byte) error {
    fmt.Printf("收到消息: %s\n", string(msg))
    return nil
})

consumer.Start()

// 优雅停止消费者
ctx := consumer.Stop()
<-ctx.Done()
```

## 优雅关闭
确保在程序结束时调用：
```go
defer amqpx.Close()
```
以关闭所有连接和活动的消费者。

