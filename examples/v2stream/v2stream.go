package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/common"
	"github.com/alpacahq/alpaca-trade-api-go/v2/stream"
)

func main() {
	// You can set your credentials here in the code, or (preferably) via the
	// APCA_API_KEY_ID and APCA_API_SECRET_KEY environment variables
	apiKey := "YOUR_API_KEY_HERE"
	apiSecret := "YOUR_API_SECRET_HERE"
	if common.Credentials().ID == "" {
		os.Setenv(common.EnvApiKeyID, apiKey)
	}
	if common.Credentials().Secret == "" {
		os.Setenv(common.EnvApiSecretKey, apiSecret)
	}

	client, err := stream.NewClient()
	if err != nil {
		panic(err)
	}

	// uncomment if you have PRO subscription
	// client.UseFeed("sip")

	client.SubscribeTrades(tradeHandler, "AAPL")
	client.SubscribeQuotes(quoteHandler, "MSFT")
	client.SubscribeBars(barHandler, "*")
	client.RegisterRestartHandler(restart)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	client.Listen(ctx)
}

func restart() {
	fmt.Println("websocket connection restarted")
}

func tradeHandler(trade stream.Trade) {
	fmt.Println("trade", trade)
}

func quoteHandler(quote stream.Quote) {
	fmt.Println("quote", quote)
}

func barHandler(bar stream.Bar) {
	fmt.Println("bar", bar)
}
