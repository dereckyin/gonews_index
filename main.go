package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"reflect"
	"time"

	"github.com/naoina/toml"
	"github.com/olivere/elastic"
)

// AppConfig for connection
type AppConfig struct {
	Pghost     string
	Pgport     int
	Pguser     string
	Pgpassword string
	Pgdbname   string
	Fmhost     string
	Fmport     int
	Fmuser     string
	Fmpassword string
	Fmdbname   string
	Myhost     string
	Myport     int
	Myuser     string
	Mypassword string
	Mydbname   string
}

// Config for environment
type Config struct {
	Dev  AppConfig
	Prod AppConfig
}

// ProductCotent (Models)
type ProductCotent struct {
	ID            int64  `json:"id"`
	Pn            string `json:"pn"`
	SupplierPn    string `json:"supplier_pn"`
	Mfs           string `json:"mfs"`
	Catalog       string `json:"catalog"`
	Description   string `json:"description"`
	Param         string `json:"param"`
	Supplier      string `json:"supplier"`
	Inventory     string `json:"inventory"`
	Currency      string `json:"currency"`
	OfficialPrice string `json:"official_price"`
}

// DesignContent (Models)
type DesignContent struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	Mfs        string `json:"mfs"`
	Category   string `json:"category"`
	Pn         string `json:"pn"`
	Desc       string `json:"desc"`
	Feature    string `json:"features"`
	Logo       string `json:"logo"`
	URL        string `json:"url"`
	TotalCount int64  `json:"total_count"`
	Product    string `json:"product"`
}

// NewsContent (Models)
type NewsContent struct {
	ID             int64     `json:"id"`
	Picture        string    `json:"picture"`
	MainTitle      string    `json:"main_title"`
	Content        string    `json:"content"`
	ArticleContent string    `json:"article_content"`
	ArticleWeb     string    `json:"article_web"`
	CreateTime     time.Time `json:"create_time"`
	TimeString     string    `json:"time_string"`
	TotalCount     int64     `json:"total_count"`
}

// DesignSearch (Models)
type DesignSearch struct {
	Took     int  `json:"took"`
	TimedOut bool `json:"timed_out"`
	Shards   struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Skipped    int `json:"skipped"`
		Failed     int `json:"failed"`
	} `json:"_shards"`

	Hits struct {
		Total    int     `json:"total"`
		MaxScore float64 `json:"max_score"`
		Hits     []struct {
			Index  string  `json:"_index"`
			Type   string  `json:"_type"`
			ID     string  `json:"_id"`
			Score  float64 `json:"_score"`
			Source struct {
				ID         int64  `json:"id"`
				Name       string `json:"name"`
				Mfs        string `json:"mfs"`
				Category   string `json:"category"`
				Pn         string `json:"pn"`
				Desc       string `json:"desc"`
				Feature    string `json:"features"`
				Logo       string `json:"logo"`
				URL        string `json:"url"`
				TotalCount int64  `json:"total_count"`
			} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

var (
	elasticClient *elastic.Client
)

var (
	dbpm *sql.DB
)

var (
	appConfig AppConfig
)

var myClient = &http.Client{Timeout: 10 * time.Second}

// CONFIG is for file name
const CONFIG = "config.toml"

func loadAppConfig(config Config, env string) AppConfig {
	r := reflect.ValueOf(config)
	return reflect.Indirect(r).FieldByName(env).Interface().(AppConfig)
}

func settingConfig() {
	fmt.Printf("Loading config file: %s\n", CONFIG)
	configData, err := ioutil.ReadFile(CONFIG)
	if err != nil {
		panic(err)
	}

	var config Config
	err = toml.Unmarshal(configData, &config)
	if err != nil {
		panic(err)
	}

	env := os.Getenv("APP_ENV")
	if env == "" {
		env = "Dev"
	}
	appConfig := loadAppConfig(config, env)
	fmt.Printf("%#v\n", appConfig)
	fmt.Printf("Environment set to %s\n", env)
}

func main() {
	var err error

	settingConfig()

	dbpm, err = ConnectPM(appConfig.Pghost, appConfig.Pgport, appConfig.Pguser, appConfig.Pgpassword, appConfig.Pgdbname)
	checkErr(err)
	defer ClosePM()

	initElastic()

	indexProduct()
	//indexDesign()
	//indexApplication()
	//indexNews()
	// searchElastic("hello world")
}

func searchElastic(qry string) {

	ctx := context.Background()

	term1Query := elastic.NewMultiMatchQuery("apple", "name", "mfs", "category", "pn", "desc", "features").Type("phrase_prefix")
	term2Query := elastic.NewMultiMatchQuery("usb", "name", "mfs", "category", "pn", "desc", "features").Type("phrase_prefix")

	generalQ := elastic.NewBoolQuery().Should().
		Filter(term1Query).Filter(term2Query)

	searchResult, err := elasticClient.Search().
		Index("design").   // search in index "twitter"
		Query(generalQ).   // specify the query
		Sort("id", false). // sort by "user" field, ascending
		From(0).Size(10).  // take documents 0-9
		Pretty(true).      // pretty print request and response JSON
		Do(ctx)            // execute
	if err != nil {
		// Handle error
		panic(err)
	}

	// searchResult is of type SearchResult and returns hits, suggestions,
	// and all kinds of other information from Elasticsearch.
	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)

	// Each is a convenience function that iterates over hits in a search result.
	// It makes sure you don't need to check for nil values in the response.
	// However, it ignores errors in serialization. If you want full control
	// over iterating the hits, see below.
	var ttyp DesignContent
	for _, item := range searchResult.Each(reflect.TypeOf(ttyp)) {
		if t, ok := item.(DesignContent); ok {
			fmt.Printf("Tweet by %d: %s\n", t.ID, t.Category)
		}
	}

	fmt.Printf("Found a total of %d tweets\n", searchResult.TotalHits())

	// Here's how you iterate through results with full control over each step.
	if searchResult.Hits.TotalHits > 0 {
		fmt.Printf("Found a total of %d tweets\n", searchResult.Hits.TotalHits)

		// Iterate through results
		for _, hit := range searchResult.Hits.Hits {
			// hit.Index contains the name of the index

			// Deserialize hit.Source into a Tweet (could also be just a map[string]interface{}).
			var t DesignContent
			err := json.Unmarshal(*hit.Source, &t)
			if err != nil {
				// Deserialization failed
			}

			// Work with tweet
			fmt.Printf("Tweet by %d: %s\n", t.ID, t.Category)
		}
	} else {
		// No hits
		fmt.Print("Found no tweets\n")
	}

	s := `{"match_all":{}}`

	res, err := elasticClient.Search().
		Index("design").
		Query(elastic.RawStringQuery(s)).
		Sort("id", false).
		Do(ctx)

	if err != nil {
		panic(err)
	}

	fmt.Printf("%d results\n", res.TotalHits())

	result := new(DesignSearch) // or &Foo{}
	getJson("http://192.168.3.131:9200/design/_search?pretty&size=10&from=0&q=usb+apple", result)

	if result.Hits.Total > 0 {
		for _, thehit := range result.Hits.Hits {
			var t DesignContent

			t.ID = thehit.Source.ID
			t.Category = thehit.Source.Category

			// Work with tweet
			fmt.Printf("Tweet by %d: %s\n", t.ID, t.Category)
		}
	}

}

func getJson(url string, target interface{}) error {
	r, err := myClient.Get(url)
	if err != nil {
		return err
	}
	defer r.Body.Close()

	return json.NewDecoder(r.Body).Decode(target)
}

func initElastic() {
	var err error
	for {
		elasticClient, err = elastic.NewClient(
			elastic.SetURL("http://192.168.3.131:9200"),
			elastic.SetSniff(false),
		)
		if err != nil {
			log.Println(err)
			time.Sleep(3 * time.Second)
		} else {
			break
		}
	}
}

func insertProduct(docs []ProductCotent) {

	ctx := context.Background()

	for _, doc := range docs {

		_, err := elasticClient.Index().
			Index("product").
			Type("product").
			BodyJson(doc).
			Do(ctx)

		checkErr(err)
	}

}

func insertDesign(docs []DesignContent) {

	ctx := context.Background()

	for _, doc := range docs {

		_, err := elasticClient.Index().
			Index("mfs").
			Type("design").
			BodyJson(doc).
			Do(ctx)

		checkErr(err)
	}

}

func insertApplication(docs []DesignContent) {

	ctx := context.Background()

	for _, doc := range docs {

		_, err := elasticClient.Index().
			Index("mfs").
			Type("app").
			BodyJson(doc).
			Do(ctx)

		checkErr(err)
	}

}

func insertNews(docs []NewsContent) {

	ctx := context.Background()

	for _, doc := range docs {

		_, err := elasticClient.Index().
			Index("news").
			Type("news").
			BodyJson(doc).
			Do(ctx)

		checkErr(err)
	}

}

func indexApplication() {

	var records = []DesignContent{}

	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()

	sqlstr := fmt.Sprintf("SELECT spider_mfs_application.id, coalesce(name, '') \"name\", coalesce(spider_mfs_application.mfs, '') mfs, coalesce(category, '') category,  '' product_name, coalesce(spider_mfs_application.\"desc\", '') \"desc\", coalesce(features, '') features, coalesce(product, '') product from spider_mfs_application left join spider_mfs_application_product on spider_mfs_application.id = spider_mfs_application_product.id")

	//fmt.Print(sqlstr)

	rows, err := dbpm.Query(sqlstr)
	checkErr(err)

	defer rows.Close()

	//time.Sleep(time.Duration(20) * time.Second)

	for rows.Next() {
		var content DesignContent

		err = rows.Scan(&content.ID, &content.Name, &content.Mfs, &content.Category, &content.Pn, &content.Desc, &content.Feature, &content.Product)
		checkErr(err)
		content.TotalCount = 1
		records = append(records, content)
	}

	insertApplication(records)
}

func indexDesign() {

	var records = []DesignContent{}

	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()

	sqlstr := fmt.Sprintf("SELECT spider_mfs_design.id, name, coalesce(spider_mfs_design.mfs, '') mfs, coalesce(category, '') category, coalesce(product_name, '') product_name, coalesce(spider_mfs_design.\"desc\", '') \"desc\", coalesce(features, '') features, coalesce(product, '') product from spider_mfs_design left join spider_mfs_design_product on spider_mfs_design.id = spider_mfs_design_product.id")

	//fmt.Print(sqlstr)

	rows, err := dbpm.Query(sqlstr)
	checkErr(err)

	defer rows.Close()

	//time.Sleep(time.Duration(20) * time.Second)

	for rows.Next() {
		var content DesignContent

		err = rows.Scan(&content.ID, &content.Name, &content.Mfs, &content.Category, &content.Pn, &content.Desc, &content.Feature, &content.Product)
		checkErr(err)
		content.TotalCount = 2
		records = append(records, content)
	}

	insertDesign(records)
}

func indexProduct() {

	var records = []ProductCotent{}

	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()

	sqlstr := fmt.Sprintf(`SELECT id, pn, supplier_pn, mfs, catalog, description, param, supplier, inventory, currency, offical_price FROM (
		SELECT b.id, b.pn, b.supplier_pn, CASE WHEN trim(d.NAME) <> '' THEN d.NAME ELSE b.mfs END as mfs, b.catalog,  coalesce(b.description, '') description, coalesce(b.param,'') param, coalesce(c.name, '') supplier, coalesce(a.inventory, 0) inventory, coalesce(a.currency, '') currency, coalesce(a.offical_price, '') offical_price
		FROM pm_product b  LEFT JOIN pm_store_price_select a on a.product_id = b.id  LEFT JOIN pm_product_config e on(e.supplier_id=b.supplier_id) 
		LEFT JOIN pm_mfs_standard d on (b.mfs_id = d.id),  pm_supplier c   where b.supplier_id = c.id AND b.status is null and (c.status='1' OR c.status  IS NULL) 
		union  
		SELECT b.id, b.pn, b.supplier_pn, CASE WHEN trim(d.NAME) <> '' THEN d.NAME ELSE b.mfs END as mfs, b.catalog,  coalesce(b.description, '') description, coalesce(b.param,'') param, coalesce(c.name, '') supplier, coalesce(a.inventory, 0) inventory, coalesce(a.currency, '') currency, coalesce(a.offical_price, '') offical_price
		FROM pm_supplier_product_c1s b  LEFT JOIN pm_store_price_select a on a.product_id = b.id  LEFT JOIN pm_product_config e on(e.supplier_id=b.supplier_id) 
		LEFT JOIN pm_mfs_standard d on (b.mfs_id = d.id),  pm_supplier c   where b.supplier_id = c.id AND b.status is null and (c.status='1' OR c.status  IS NULL) 
		union  
		SELECT b.id, b.pn, b.supplier_pn, CASE WHEN trim(d.NAME) <> '' THEN d.NAME ELSE b.mfs END as mfs, b.catalog,  coalesce(b.description, '') description, coalesce(b.param,'') param, coalesce(c.name, '') supplier, coalesce(a.inventory, 0) inventory, coalesce(a.currency, '') currency, coalesce(a.offical_price, '') offical_price
		FROM pm_supplier_product_octopart b  LEFT JOIN pm_store_price_select a on a.product_id = b.id  LEFT JOIN pm_product_config e on(e.supplier_id=b.supplier_id) 
		LEFT JOIN pm_mfs_standard d on (b.mfs_id = d.id),  pm_supplier c   where b.supplier_id = c.id AND b.status is null and (c.status='1' OR c.status  IS NULL) 
		union  
		SELECT b.id, b.pn, b.supplier_pn, CASE WHEN trim(d.NAME) <> '' THEN d.NAME ELSE b.mfs END as mfs, b.catalog,  coalesce(b.description, '') description, coalesce(b.param,'') param, coalesce(c.name, '') supplier, coalesce(a.inventory, 0) inventory, coalesce(a.currency, '') currency, coalesce(a.offical_price, '') offical_price
		FROM pm_supplier_product_findchips b  LEFT JOIN pm_store_price_select a on a.product_id = b.id  LEFT JOIN pm_product_config e on(e.supplier_id=b.supplier_id) 
		LEFT JOIN pm_mfs_standard d on (b.mfs_id = d.id),  pm_supplier c   where b.supplier_id = c.id AND b.status is null and (c.status='1' OR c.status  IS NULL) 
		union  
		SELECT b.id, b.pn, b.supplier_pn, CASE WHEN trim(d.NAME) <> '' THEN d.NAME ELSE b.mfs END as mfs, b.catalog,  coalesce(b.description, '') description, coalesce(b.param,'') param, coalesce(c.name, '') supplier, coalesce(a.inventory, 0) inventory, coalesce(a.currency, '') currency, coalesce(a.offical_price, '') offical_price
		FROM pm_supplier_product_463 b  LEFT JOIN pm_store_price_select a on a.product_id = b.id  LEFT JOIN pm_product_config e on(e.supplier_id=b.supplier_id) 
		LEFT JOIN pm_mfs_standard d on (b.mfs_id = d.id),  pm_supplier c   where b.supplier_id = c.id AND b.status is null and (c.status='1' OR c.status  IS NULL) 
		union  
		SELECT b.id, b.pn, b.supplier_pn, CASE WHEN trim(d.NAME) <> '' THEN d.NAME ELSE b.mfs END as mfs, b.catalog,  coalesce(b.description, '') description, coalesce(b.param,'') param, coalesce(c.name, '') supplier, coalesce(a.inventory, 0) inventory, coalesce(a.currency, '') currency, coalesce(a.offical_price, '') offical_price
		FROM pm_supplier_product_findic b  LEFT JOIN pm_store_price_select a on a.product_id = b.id  LEFT JOIN pm_product_config e on(e.supplier_id=b.supplier_id) 
		LEFT JOIN pm_mfs_standard d on (b.mfs_id = d.id),  pm_supplier c   where b.supplier_id = c.id AND b.status is null and (c.status='1' OR c.status  IS NULL) 
		union  
		SELECT b.id, b.pn, b.supplier_pn, CASE WHEN trim(d.NAME) <> '' THEN d.NAME ELSE b.mfs END as mfs, b.catalog,  coalesce(b.description, '') description, coalesce(b.param,'') param, coalesce(c.name, '') supplier, coalesce(a.inventory, 0) inventory, coalesce(a.currency, '') currency, coalesce(a.offical_price, '') offical_price
		FROM pm_supplier_product_ickey b  LEFT JOIN pm_store_price_select a on a.product_id = b.id  LEFT JOIN pm_product_config e on(e.supplier_id=b.supplier_id) 
		LEFT JOIN pm_mfs_standard d on (b.mfs_id = d.id),  pm_supplier c   where b.supplier_id = c.id AND b.status is null and (c.status='1' OR c.status  IS NULL) 
		) result`)

	//fmt.Print(sqlstr)

	rows, err := dbpm.Query(sqlstr)
	checkErr(err)

	defer rows.Close()

	//time.Sleep(time.Duration(20) * time.Second)

	for rows.Next() {
		var content ProductCotent

		err = rows.Scan(&content.ID, &content.Pn, &content.SupplierPn, &content.Mfs, &content.Catalog, &content.Description, &content.Param, &content.Supplier, &content.Inventory, &content.Currency, &content.OfficialPrice)
		checkErr(err)

		records = append(records, content)
	}

	insertProduct(records)
}

func indexNews() {
	dbmy, err := Connect(appConfig.Myhost, appConfig.Myport, appConfig.Myuser, appConfig.Mypassword, appConfig.Mydbname)
	checkErr(err)
	defer Close()

	var records = []NewsContent{}

	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()

	sqlstr := fmt.Sprintf("select news_article.id, '' picture, main_title, content, '' article_content, '' article_web, news_article.create_time, date_format(news_article.create_time, '%Y/%m/%d') time_string from news_article inner join news_article_content on news_article.id = news_article_content.article_id")

	//fmt.Print(sqlstr)

	rows, err := dbmy.Query(sqlstr)
	checkErr(err)

	defer rows.Close()

	//time.Sleep(time.Duration(20) * time.Second)

	for rows.Next() {
		var content NewsContent

		err = rows.Scan(&content.ID, &content.Picture, &content.MainTitle, &content.Content, &content.ArticleContent, &content.ArticleWeb, &content.CreateTime, &content.TimeString)
		checkErr(err)
		content.TotalCount = 3
		records = append(records, content)
	}

	insertNews(records)
}

func checkErr(err error) {
	if err != nil {
		log.Printf("%s", err)
		panic(err)
	}
}
