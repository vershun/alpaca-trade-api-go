package stream

import (
	"errors"
	"fmt"

	"github.com/alpacahq/alpaca-trade-api-go/common"
	"github.com/mitchellh/mapstructure"
	"github.com/vmihailenco/msgpack/v5"
)

func (c *Client) handleMessage(msg map[string]interface{}) error {
	dataType, ok := msg["T"].(string)
	if !ok {
		return errors.New("T missing")
	}

	switch string(dataType) {
	case "success", "error":
		err := c.handleStatusMessage(msg)
		if err != nil {
			c.Restart()
		}
		return err

	case "subscription":
		return nil

	case "q", "t", "b":
		return c.handleDataMessage(msg)
	}

	return errors.New("unknown " + dataType)
}

func (c *Client) handleStatusMessage(msg map[string]interface{}) error {
	dataType := msg["T"].(string)
	status, ok := msg["msg"].(string)
	if !ok {
		return errors.New("T missing")
	}

	if dataType == "error" {
		return fmt.Errorf("error %s", status)
	}

	switch status {
	case "connected":
		b, err := msgpack.Marshal(map[string]string{
			"action": "auth",
			"key":    common.Credentials().ID,
			"secret": common.Credentials().Secret,
		})
		if err != nil {
			return err
		}

		c.out <- b
		return nil

	case "authenticated":
		trades := make([]string, 0, len(c.tradeHandlers))
		for trade := range c.tradeHandlers {
			trades = append(trades, trade)
		}
		quotes := make([]string, 0, len(c.quoteHandlers))
		for quote := range c.quoteHandlers {
			quotes = append(quotes, quote)
		}
		bars := make([]string, 0, len(c.barHandlers))
		for bar := range c.barHandlers {
			bars = append(bars, bar)
		}

		b, err := msgpack.Marshal(map[string]interface{}{
			"action": "subscribe",
			"trades": trades,
			"quotes": quotes,
			"bars":   bars,
		})
		if err != nil {
			return err
		}

		c.out <- b
		return nil

	default:
		return fmt.Errorf("unknown status %s", status)
	}
}

func (c *Client) handleDataMessage(msg map[string]interface{}) error {
	dataType := msg["T"].(string)
	symbol, ok := msg["S"].(string)
	if !ok {
		return errors.New("S missing")
	}

	switch dataType {
	case "t":
		var trade Trade
		if err := mapstructure.Decode(msg, &trade); err != nil {
			return err
		}

		handler, ok := c.tradeHandlers[symbol]
		if !ok {
			if handler, ok = c.tradeHandlers["*"]; !ok {
				return nil
			}
		}

		handler(trade)

	case "q":
		var quote Quote
		if err := mapstructure.Decode(msg, &quote); err != nil {
			return err
		}

		handler, ok := c.quoteHandlers[symbol]
		if !ok {
			if handler, ok = c.quoteHandlers["*"]; !ok {
				return nil
			}
		}

		handler(quote)

	case "b":
		var bar Bar
		if err := mapstructure.Decode(msg, &bar); err != nil {
			return err
		}

		handler, ok := c.barHandlers[symbol]
		if !ok {
			if handler, ok = c.barHandlers["*"]; !ok {
				return nil
			}
		}

		handler(bar)
	}

	return nil
}
