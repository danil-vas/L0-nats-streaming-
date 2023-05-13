package main

import (
	"encoding/json"
	"fmt"
	stan "github.com/nats-io/go-nats-streaming"
	"log"
	"os"
	"time"
)

type DeliveryJ struct {
	Name    string `json:"name"`
	Phone   string `json:"phone"`
	Zip     string `json:"zip"`
	City    string `json:"city"`
	Address string `json:"address"`
	Region  string `json:"region"`
	Email   string `json:"email"`
}

type PaymentJ struct {
	Transaction  string `json:"transaction"`
	RequestId    string `json:"request_id"`
	Currency     string `json:"currency"`
	Provider     string `json:"provider"`
	Amount       int    `json:"amount"`
	PaymentDt    int    `json:"payment_dt"`
	Bank         string `json:"bank"`
	DeliveryCost int    `json:"delivery_cost"`
	GoodsTotal   int    `json:"goods_total"`
	CustomFee    int    `json:"custom_fee"`
}

type ItemsJ struct {
	ChrtId      int    `json:"chrt_id"`
	TrackNumber string `json:"track_number"`
	Price       int    `json:"price"`
	Rid         string `json:"rid"`
	Name        string `json:"name"`
	Sale        int    `json:"sale"`
	Size        string `json:"size"`
	TotalPrice  int    `json:"total_price"`
	NmId        int    `json:"nm_id"`
	Brand       string `json:"brand"`
	Status      int    `json:"status"`
}

type Order struct {
	OrderUid          string    `json:"order_uid"`
	TrackNumber       string    `json:"track_number"`
	Entry             string    `json:"entry"`
	Delivery          DeliveryJ `json:"delivery"`
	Payment           PaymentJ  `json:"payment"`
	Items             []ItemsJ  `json:"items"`
	Locale            string    `json:"locale"`
	InternalSignature string    `json:"internal_signature"`
	CustomerId        string    `json:"customer_id"`
	DeliveryService   string    `json:"delivery_service"`
	Shardkey          string    `json:"shardkey"`
	SmId              int       `json:"sm_id"`
	DateCreated       time.Time `json:"date_created"`
	OofShard          string    `json:"oof_shard"`
}

func main() {
	sc, err := stan.Connect("prod", "simple-pub", stan.NatsURL(stan.DefaultNatsURL))
	if err != nil {
		log.Print(err)
		return
	}
	defer sc.Close()
	var item Order
	obj, _ := os.ReadFile("model.json")
	err = json.Unmarshal(obj, &item)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(item)
	t, err := json.Marshal(item)
	for {
		sc.Publish("canal", t)
		log.Println("Published message on channel: " + "canal")
		time.Sleep(60 * time.Second)
	}
}
