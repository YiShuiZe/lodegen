package lodegen

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"lodegen/lib"
	"lodegen/log"
	"math"
	"sync/atomic"
	"time"
)

// 日志记录器
// var logger = log.DLogger()
var logger = log.DLogger()

// myGenerator 代表载荷发生器的实现类型
type myGenerator struct {
	caller      lib.Caller           // 调用器
	timeoutNS   time.Duration        // 处理超时时间，单位：纳秒  超过此时间的请求，被认为是无效的
	lps         uint32               // 每秒载荷量 				每秒发送载荷的数量
	durationNS  time.Duration        // 负载持续时间，单位：纳秒  持续发送载荷的时间。
	concurrency uint32               // 载荷并发量
	tickets     lib.GoTickets        // Goroutine票池
	ctx         context.Context      // 上下文
	cancelFunc  context.CancelFunc   // 取消函数
	callCount   int64                // 调用计数
	status      uint32               // 状态					载荷发生器的状态
	resultCh    chan *lib.CallResult // 调用结果通道   			 结果列表的输出是并发的，不能使用数组或者切片，因为它们不是并发安全的，原生的数据类型中只有 通道Channel 是并发安全的
}

// NewGenerator 新建一个载荷发生器
func NewGenerator(pset ParamSet) (lib.Generator, error) {

	logger.Infoln("New a load generator...")
	if err := pset.Check(); err != nil {
		return nil, err
	}
	gen := &myGenerator{
		caller:     pset.Caller,
		timeoutNS:  pset.TimeoutNS,
		lps:        pset.LPS,
		durationNS: pset.DurationNS,
		status:     lib.STATUS_ORIGINAL,
		resultCh:   pset.ResultCh,
	}
	if err := gen.init(); err != nil {
		return nil, err
	}
	return gen, nil

}

// 初始化载荷发生器
func (gen *myGenerator) init() error {
	var buf bytes.Buffer
	buf.WriteString("Initializing the load generator...")
	// 载荷的并发量 = 载荷的响应超时时间 / 载荷的发送间隔时间
	var total64 = int64(gen.timeoutNS)/int64(1e9/gen.lps) + 1

	if total64 > math.MaxInt32 {
		total64 = math.MaxInt32
	}
	gen.concurrency = uint32(total64)
	tickets, err := lib.NewGoTickets(gen.concurrency)
	if err != nil {
		return err
	}
	gen.tickets = tickets

	buf.WriteString(fmt.Sprintf("Done. (concurrency=%d)", gen.concurrency))
	logger.Infoln(buf.String())
	return nil
}

// callOne 会向载荷承受方发起一次调用
func (gen *myGenerator) callOne(rawReq *lib.RawReq) *lib.RawResp {
	atomic.AddInt64(&gen.callCount, 1)
	if rawReq == nil {
		return &lib.RawResp{ID: -1, Err: errors.New("Invalid raw request.")}
	}
	// 开始调用时间
	start := time.Now().UnixNano()
	// 调用
	resp, err := gen.caller.Call(rawReq.Req, gen.timeoutNS)
	// 结束调用时间
	end := time.Now().UnixNano()
	// 调用响应时间
	elapsedTime := time.Duration(end - start)
	var rawResp lib.RawResp
	if err != nil {
		errMsg := fmt.Sprintf("Sync Call Error: %s.", err)
		rawResp = lib.RawResp{
			ID:     rawReq.ID,
			Err:    errors.New(errMsg),
			Elapse: elapsedTime,
		}
	} else {
		rawResp = lib.RawResp{
			ID:     rawReq.ID,
			Resp:   resp,
			Elapse: elapsedTime,
		}
	}
	return &rawResp
}

// asynSend 会异步地调用承受方接口
func (gen *myGenerator) asyncCall() {
	gen.tickets.Take() // 从票池中获取一个票据
	go func() {
		defer func() { // defer 处理
			if p := recover(); p != nil { // panic恐慌导致进入defer语句
				err, ok := interface{}(p).(error) // 类型断言p是不是error
				var errMsg string
				if ok { // p是error
					errMsg = fmt.Sprintf("Async Call Panic! (error: %s)", err)
				} else {
					errMsg = fmt.Sprintf("Async Call Panic! (clue: %#v)", p)
				}
				logger.Errorln(errMsg)
				result := &lib.CallResult{ // 生成调用结果
					ID:   -1,
					Code: lib.RET_CODE_FATAL_CALL,
					Msg:  errMsg,
				}
				gen.sendResult(result)
			}
			gen.tickets.Return() // 最后，将票据 送回 票池
		}()

		rawReq := gen.caller.BuildReq() // 1. 生成载荷   由调用器处理
		// 调用状态： 0-未调用或调用中；1-调用完成；2-调用超时。
		var callStatus uint32
		timer := time.AfterFunc(gen.timeoutNS, func() { // 定时器  处理 响应超时
			if !atomic.CompareAndSwapUint32(&callStatus, 0, 2) {
				return
			}
			result := &lib.CallResult{
				ID:     rawReq.ID,
				Req:    rawReq,
				Code:   lib.RET_CODE_WARNING_CALL_TIMEOUT,
				Msg:    fmt.Sprintf("Timeout! (expected: < %v)", gen.timeoutNS),
				Elapse: gen.timeoutNS, // 响应处理时间 为 限定的超时时间
			}
			gen.sendResult(result)
		})
		rawResp := gen.callOne(&rawReq) // 2. 发起调用操作，得到响应
		if !atomic.CompareAndSwapUint32(&callStatus, 0, 1) {
			return // 说明调用超时，直接返回即可，因为超时的话，上面的超时定时器已经处理了
		}
		// 停掉定时器
		timer.Stop()
		var result *lib.CallResult
		if rawResp.Err != nil {
			result = &lib.CallResult{
				ID:     rawResp.ID,
				Req:    rawReq,
				Code:   lib.RET_CODE_ERROR_CALL,
				Msg:    rawResp.Err.Error(),
				Elapse: rawResp.Elapse,
			}
		} else {
			result = gen.caller.CheckResp(rawReq, *rawResp) // 检查响应
			result.Elapse = rawResp.Elapse
		}
		gen.sendResult(result)
	}()
}

// sendResult 用于发送调用结果
func (gen *myGenerator) sendResult(result *lib.CallResult) bool {
	if atomic.LoadUint32(&gen.status) != lib.STATUS_STARTED {
		gen.printIgnoreResult(result, "stopped load generator")
		return false
	}
	select {
	case gen.resultCh <- result: // 将调用结果 写入到 gen中的 结果缓冲通道
		return true
	default: // gen中的 结果缓冲通道已满
		gen.printIgnoreResult(result, "full result channel")
		return false
	}
}

// printIgnoreResult 打印被忽略的结果
func (gen *myGenerator) printIgnoreResult(result *lib.CallResult, cause string) {
	//resultMsg := fmt.Sprintf("ID=%d, Code=%d, Msg=%s, Elapse=%v", result.ID, result.Code, result.Msg, result.Elapse)
	fmt.Sprintf("ID=%d, Code=%d, Msg=%s, Elapse=%v", result.ID, result.Code, result.Msg, result.Elapse)
	// logger.Warnf("Ignored result: %s. (cause: %s)\n", resultMsg, cause)
}

// prepareToStop 用于为停止载荷发生器做准备
func (gen *myGenerator) prepareToStop(ctxError error) {
	logger.Infof("Prepare to stop load generator (cause: %s)...", ctxError)
	atomic.CompareAndSwapUint32(&gen.status, lib.STATUS_STARTED, lib.STATUS_STOPPING)
	logger.Infof("Closing result channel...")
	close(gen.resultCh)
	atomic.StoreUint32(&gen.status, lib.STATUS_STOPPED)
}

// genLoad 会产生载荷并向承受方发送   控制调用流程
func (gen *myGenerator) genLoad(throttle <-chan time.Time) {
	for { // 循环周期性地向被测软件发送载荷 （周期长短由节流阀控制）
		select {
		case <-gen.ctx.Done(): // 停止信号
			gen.prepareToStop(gen.ctx.Err())
			return
		default:
		}
		gen.asyncCall() // 异步的一次调用
		if gen.lps > 0 {
			select {
			case <-throttle: // 调用的间隔时间到了，就会跳出去，开始下一次循环，下一次调用
			case <-gen.ctx.Done():
				gen.prepareToStop(gen.ctx.Err())
				return
			}
		}
	}
}

// Start 会启动载荷发生器
func (gen *myGenerator) Start() bool {
	logger.Infoln("Starting load generator...")
	// 检查是否具备可启动的状态，顺便设置状态为正在启动
	if !atomic.CompareAndSwapUint32(&gen.status, lib.STATUS_ORIGINAL, lib.STATUS_STARTING) {
		if !atomic.CompareAndSwapUint32(&gen.status, lib.STATUS_STOPPED, lib.STATUS_STARTING) {
			return false
		}
	}

	// 设定节流阀
	var throttle <-chan time.Time
	if gen.lps > 0 {
		interval := time.Duration(1e9 / gen.lps)
		logger.Infof("Setting throttle (%v)...", interval)
		throttle = time.Tick(interval)
	}

	// 初始化上下文和取消函数
	gen.ctx, gen.cancelFunc = context.WithTimeout(context.Background(), gen.durationNS)

	// 初始化调用计数
	gen.callCount = 0

	// 设置状态为已启动
	atomic.StoreUint32(&gen.status, lib.STATUS_STARTED)

	go func() {
		// 生成并发送载荷
		logger.Infoln("Generating loads...")
		gen.genLoad(throttle)
		logger.Infof("Stopped. (call count: %d)", gen.callCount)
	}()
	return true
}

func (gen *myGenerator) Stop() bool {
	if !atomic.CompareAndSwapUint32(&gen.status, lib.STATUS_STARTED, lib.STATUS_STOPPING) {
		return false
	}
	gen.cancelFunc()
	for {
		if atomic.LoadUint32(&gen.status) == lib.STATUS_STOPPED {
			break
		}
		time.Sleep(time.Microsecond)
	}
	return true
}

func (gen *myGenerator) Status() uint32 {
	return atomic.LoadUint32(&gen.status)
}

func (gen *myGenerator) CallCount() int64 {
	return atomic.LoadInt64(&gen.callCount)
}
