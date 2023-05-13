package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	stan "github.com/nats-io/go-nats-streaming"
	"github.com/patrickmn/go-cache"
	_ "github.com/patrickmn/go-cache"
	"log"
	"net/http"
	_ "net/http"
	"runtime"
	"time"
)

type DeliveryJ struct {
	Id      int    `json:"-"`
	Name    string `json:"name"`
	Phone   string `json:"phone"`
	Zip     string `json:"zip"`
	City    string `json:"city"`
	Address string `json:"address"`
	Region  string `json:"region"`
	Email   string `json:"email"`
}

type PaymentJ struct {
	Id           int    `json:"-"`
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
	Id          int    `json:"-"`
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
	Id                int       `json:"-"`
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

var inMem *cache.Cache

func AddDB(order *Order, db *sql.DB) {
	var idDelivery int
	addDelivery := fmt.Sprintf("insert into delivery (name, phone, zip, city, address, region, email)" +
		" values ($1, $2, $3, $4, $5, $6, $7) returning id;")
	db.QueryRow(addDelivery, order.Delivery.Name, order.Delivery.Phone, order.Delivery.Zip,
		order.Delivery.City, order.Delivery.Address, order.Delivery.Region,
		order.Delivery.Email).Scan(&idDelivery)
	order.Delivery.Id = idDelivery
	var idPayment int
	addPayment := fmt.Sprintf("insert into payment (transaction, request_id, currency, provider," +
		" amount, payment_dt, bank, delivery_cost, goods_total, custom_fee) " +
		"values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) returning id;")
	db.QueryRow(addPayment, order.Payment.Transaction, order.Payment.RequestId, order.Payment.Currency,
		order.Payment.Provider, order.Payment.Amount, order.Payment.PaymentDt, order.Payment.Bank,
		order.Payment.DeliveryCost, order.Payment.GoodsTotal, order.Payment.CustomFee).Scan(&idPayment)
	order.Payment.Id = idPayment
	var idOrder int
	addOrder := fmt.Sprintf("insert into public.order (order_uid, track_number, entry, delivery_id, payment_id," +
		" locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard)" +
		" values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) returning id;")
	db.QueryRow(addOrder, order.OrderUid, order.TrackNumber, order.Entry, idDelivery, idPayment, order.Locale,
		order.InternalSignature, order.CustomerId, order.DeliveryService, order.Shardkey, order.SmId, order.DateCreated, order.OofShard).Scan(&idOrder)
	order.Id = idOrder

	for _, item := range order.Items {
		addItem := fmt.Sprintf("insert into items (order_id, chrt_id, track_number, price, rid, name, sale, size," +
			" total_price, nm_id, brand, status) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);")
		_, err := db.Exec(addItem, idOrder, item.ChrtId, item.TrackNumber, item.Price, item.Rid, item.Name, item.Sale,
			item.Size, item.TotalPrice, item.NmId, item.Brand, item.Status)
		if err != nil {
			log.Print(err)
		}
	}
	fmt.Print("Success added DB\n")
}

func GetDB(rows *sql.Rows, db *sql.DB) Order {
	var r Order
	err := rows.Scan(&r.Id, &r.OrderUid, &r.TrackNumber, &r.Entry, &r.Delivery.Id, &r.Payment.Id,
		&r.Locale, &r.InternalSignature, &r.CustomerId, &r.DeliveryService, &r.Shardkey, &r.SmId, &r.DateCreated, &r.OofShard)
	if err != nil {
		log.Fatal(err)
	}
	rowDelivery, err := db.Query("select * from delivery where id = $1;", r.Delivery.Id)
	if err != nil {
		log.Fatal(err)
	} else {
		for rowDelivery.Next() {
			err := rowDelivery.Scan(&r.Delivery.Id, &r.Delivery.Name, &r.Delivery.Phone, &r.Delivery.Zip, &r.Delivery.City,
				&r.Delivery.Address, &r.Delivery.Region, &r.Delivery.Email)
			if err != nil {
				log.Fatal(err)
				continue
			}
		}
	}
	rowPayment, err := db.Query("select * from payment where id = $1;", r.Payment.Id)
	if err != nil {
		log.Fatal(err)
	} else {
		for rowPayment.Next() {
			err := rowPayment.Scan(&r.Payment.Id, &r.Payment.Transaction, &r.Payment.RequestId, &r.Payment.Currency,
				&r.Payment.Provider, &r.Payment.Amount, &r.Payment.PaymentDt, &r.Payment.Bank, &r.Payment.DeliveryCost, &r.Payment.GoodsTotal, &r.Payment.CustomFee)
			if err != nil {
				log.Fatal(err)
				continue
			}
		}
	}
	rowsItems, err := db.Query("select id, chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status from items where order_id = $1;", r.Id)
	if err != nil {
		log.Fatal(err)
	} else {
		for rowsItems.Next() {
			var i ItemsJ
			err := rowsItems.Scan(&i.Id, &i.ChrtId, &i.TrackNumber, &i.Price, &i.Rid, &i.Name, &i.Sale, &i.Size, &i.TotalPrice, &i.NmId, &i.Brand, &i.Status)
			if err != nil {
				log.Fatal(err)
				continue
			}
			r.Items = append(r.Items, i)
		}
	}
	return r
}

func getDataById(c *gin.Context) {
	id := c.Param("id")
	i, _ := inMem.Get("data")
	for _, item := range i.([]Order) {
		if item.OrderUid == id {
			c.IndentedJSON(http.StatusOK, item)
			return
		}
	}
	c.IndentedJSON(http.StatusNotFound, gin.H{"message": "order not found"})
}

func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Credentials", "true")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Header("Access-Control-Allow-Methods", "POST,HEAD,PATCH, OPTIONS, GET, PUT")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	}
}

func startServer() {
	router := gin.New()
	router.Use(CORSMiddleware())
	router.GET("/data/:id", getDataById)
	router.Run("localhost:8080")
}

func main() {
	order := make([]Order, 0)
	sc, err := stan.Connect("prod", "sub-1", stan.NatsURL(stan.DefaultNatsURL))
	if err != nil {
		log.Fatal(err)
	}
	dbData := "user=postgres password=root dbname=L0 sslmode=disable"
	db, err := sql.Open("postgres", dbData)
	if err != nil {
		log.Fatal(err)
		return
	} else {
		fmt.Print("Success connect to postgres DB\n")
	}
	inMem = cache.New(0, 0)
	rows, err := db.Query("select * from public.order;")
	if err != nil {
		log.Fatal(err)
		return
	} else {
		for rows.Next() {
			r := GetDB(rows, db)
			order = append(order, r)
		}
	}
	inMem.Set("data", order, cache.DefaultExpiration)
	go startServer()
	aw, _ := time.ParseDuration("60s")
	var newOrder Order
	sc.Subscribe("canal", func(msg *stan.Msg) {
		msg.Ack()
		err := json.Unmarshal(msg.Data, &newOrder)
		if err != nil {
			log.Print(err)
			return
		} else {
			fmt.Print("Success json transform\n")
			AddDB(&newOrder, db)
			item, _ := inMem.Get("data")
			item = append(item.([]Order), newOrder)
			inMem.Set("data", item.([]Order), cache.DefaultExpiration)
		}
	}, stan.DurableName("dur"),
		stan.MaxInflight(25),
		stan.SetManualAckMode(),
		stan.AckWait(aw))
	runtime.Goexit()
}
