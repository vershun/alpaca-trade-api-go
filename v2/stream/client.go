package stream

import (
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"nhooyr.io/websocket"
)

const (
	defaultDataStreamURL = "https://stream.data.alpaca.markets/v2/"

	ioTimeout    = 10 * time.Second
	pingPeriod   = (ioTimeout * 9) / 10
	restartDelay = 3 * time.Second
)

type tradeSubscription struct {
	handler func(Trade)
	symbols []string
}

type quoteSubscription struct {
	handler func(Quote)
	symbols []string
}

type barSubscription struct {
	handler func(Bar)
	symbols []string
}

// Client represents an Alpaca Data v2 streaming websocket client
type Client struct {
	url string

	trades chan tradeSubscription
	quotes chan quoteSubscription
	bars   chan barSubscription

	tradeHandlers map[string]func(trade Trade)
	quoteHandlers map[string]func(quote Quote)
	barHandlers   map[string]func(bar Bar)

	restartListeners []func()

	once    sync.Once
	restart chan struct{}

	in     chan []byte
	out    chan []byte
	pinger chan struct{}
}

// NewClient creates a new Alpaca Data v2 streaming websocket instance
func NewClient() (*Client, error) {
	client := &Client{
		trades: make(chan tradeSubscription, 1),
		quotes: make(chan quoteSubscription, 1),
		bars:   make(chan barSubscription, 1),

		tradeHandlers: make(map[string]func(trade Trade)),
		quoteHandlers: make(map[string]func(quote Quote)),
		barHandlers:   make(map[string]func(bar Bar)),

		in:  make(chan []byte, 10000),
		out: make(chan []byte),
	}
	client.UseFeed("iex")

	return client, nil
}

// UseFeed sets the last part of the data stream URL
// "iex": for normal users
// "sip": for premium users
func (c *Client) UseFeed(feed string) {
	var url string
	if url = os.Getenv("DATA_PROXY_WS"); url == "" {
		url = defaultDataStreamURL
	}
	c.url = url + feed
}

// Restart closes the restart channel to signal controlLoop() to stop running.
// It is protected by a sync.Once as it can be closed in websocketWriter() and
// websocketReader() goroutines.
func (c *Client) Restart() {
	c.once.Do(func() {
		close(c.restart)
	})
}

// RegisterRestartHandler adds the specified function to the list which will be
// notified on websocket restarts.
func (c *Client) RegisterRestartHandler(fn func()) {
	c.restartListeners = append(c.restartListeners, fn)
}

// Listen creates a connection to the websocket and reconnects if any error arises.
// This call is blocking so the goroutine will never return. You have to call
// Close() to exit from the infinite loop.
func (c *Client) Listen(ctx context.Context) {
	for {
		wg := sync.WaitGroup{}
		c.out = make(chan []byte)

		c.once = sync.Once{}
		c.restart = make(chan struct{})
		c.pinger = make(chan struct{})

		conn, _, err := websocket.Dial(context.TODO(), c.url, &websocket.DialOptions{
			CompressionMode: websocket.CompressionContextTakeover,
			HTTPHeader:      http.Header{"Content-Type": []string{"application/msgpack"}},
		})
		if err != nil {
			log.Println("websocket connection error:", err)
			goto failed
		}

		wg.Add(4)
		go c.controlLoop(ctx, &wg)
		go c.websocketWriter(conn, &wg)
		go c.websocketReader(conn, &wg)
		go c.websocketPinger(conn, &wg)
		wg.Wait()

	failed:
		select {
		case <-ctx.Done():
			return

		default:
			for _, fn := range c.restartListeners {
				fn()
			}
			time.Sleep(restartDelay)
		}
	}
}

// SubscribeTrades adds handler as a trade handler for symbols
func (c *Client) SubscribeTrades(handler func(trade Trade), symbols ...string) {
	c.trades <- tradeSubscription{
		handler: handler,
		symbols: symbols,
	}
}

// SubscribeQuotes adds handler as a quote handler for symbols
func (c *Client) SubscribeQuotes(handler func(quote Quote), symbols ...string) {
	c.quotes <- quoteSubscription{
		handler: handler,
		symbols: symbols,
	}
}

// SubscribeBars adds handler as a bar handler for symbols
func (c *Client) SubscribeBars(handler func(bar Bar), symbols ...string) {
	c.bars <- barSubscription{
		handler: handler,
		symbols: symbols,
	}
}

func (c *Client) initHandlers() {
	for loop := true; loop == true; {
		select {
		case sub := <-c.trades:
			for _, symbol := range sub.symbols {
				c.tradeHandlers[symbol] = sub.handler
			}
		default:
			loop = false
		}
	}

	for loop := true; loop == true; {
		select {
		case sub := <-c.quotes:
			for _, symbol := range sub.symbols {
				c.quoteHandlers[symbol] = sub.handler
			}
		default:
			loop = false
		}
	}

	for loop := true; loop == true; {
		select {
		case sub := <-c.bars:
			for _, symbol := range sub.symbols {
				c.barHandlers[symbol] = sub.handler
			}
		default:
			loop = false
		}
	}
}

func (c *Client) websocketReader(conn *websocket.Conn, wg *sync.WaitGroup) {
	defer func() {
		c.Restart()
		wg.Done()
	}()

	for {
		msgType, msg, err := conn.Read(context.TODO())
		if err != nil {
			log.Println("websocket read error:", err)
			return
		}

		if msgType != websocket.MessageBinary {
			continue
		}

		c.in <- msg
	}
}

func (c *Client) websocketWriter(conn *websocket.Conn, wg *sync.WaitGroup) {
	defer func() {
		c.Restart()
		for range c.out {
		}
		conn.Close(websocket.StatusNormalClosure, "")
		wg.Done()
	}()

	for b := range c.out {
		ctx, cancel := context.WithTimeout(context.TODO(), ioTimeout)
		defer cancel()

		if err := conn.Write(ctx, websocket.MessageBinary, b); err != nil {
			log.Println("websocket write error:", err)
			return
		}
	}
}

func (c *Client) websocketPinger(conn *websocket.Conn, wg *sync.WaitGroup) {
	pinger := time.NewTicker(pingPeriod)
	defer func() {
		c.Restart()
		pinger.Stop()
		wg.Done()
	}()

	for {
		select {
		case <-c.pinger:
			return

		case <-pinger.C:
			ctx, cancel := context.WithTimeout(context.TODO(), pingPeriod)
			defer cancel()

			if err := conn.Ping(ctx); err != nil {
				log.Println("websocket ping error:", err)
				return
			}
		}
	}
}

func (c *Client) controlLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer func() {
		close(c.pinger)
		close(c.out)
		wg.Done()
	}()

	c.initHandlers()

	for {
		select {
		case <-ctx.Done():
			return

		case <-c.restart:
			return

		case sub := <-c.trades:
			for _, symbol := range sub.symbols {
				c.tradeHandlers[symbol] = sub.handler
			}

		case sub := <-c.quotes:
			for _, symbol := range sub.symbols {
				c.quoteHandlers[symbol] = sub.handler
			}

		case sub := <-c.bars:
			for _, symbol := range sub.symbols {
				c.barHandlers[symbol] = sub.handler
			}

		case b := <-c.in:
			var messages []map[string]interface{}
			if err := msgpack.Unmarshal(b, &messages); err != nil && err != io.EOF {
				log.Println("msgpack unmarshal error:", err)
				continue
			}

			for _, msg := range messages {
				if err := c.handleMessage(msg); err != nil {
					log.Println("unexpected message:", err)
				}
			}
		}
	}
}
