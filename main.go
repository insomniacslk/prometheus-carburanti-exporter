package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	flagPath          = flag.String("p", "/metrics", "HTTP path where to expose metrics to")
	flagListen        = flag.String("l", ":9112", "Address to listen to")
	flagSleepInterval = flag.Duration("i", 6*time.Hour, "Interval between data updates, expressed as a Go duration string")
)

// See https://www.mimit.gov.it/index.php/it/open-data/elenco-dataset/carburanti-prezzi-praticati-e-anagrafica-degli-impianti
const (
	pricesCSVURL   = "https://www.mimit.gov.it/images/exportCSV/prezzo_alle_8.csv"
	stationsCSVURL = "https://www.mimit.gov.it/images/exportCSV/anagrafica_impianti_attivi.csv"
)

func main() {
	flag.Parse()

	carburantiGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "osservatorio_carburanti_price",
			Help: "Fuel prices from Osservatorio Carburanti from MISE",
		},
		[]string{"IDImpianto", "Carburante", "SelfService", "Nome", "Tipo", "Comune", "Provincia", "Bandiera"},
	)
	if err := prometheus.Register(carburantiGauge); err != nil {
		log.Fatalf("Failed to register 'osservatorio_carburanti_price' gauge: %v", err)
	}

	cache := NewCache(time.Hour)

	go func() {
		for {
			records, err := refreshRecords(cache)
			if err != nil {
				log.Printf("Failed to fetch prices: %v", err)
				goto break_loop
			} else {
				// refresh the fuel stations' data
				stations, err := updateStations()
				if err != nil {
					log.Printf("failed to update stations: %v", err)
					goto break_loop
				}
				for _, record := range records {
					var nome, tipo, comune, provincia, bandiera string
					station, ok := stations[record.IDImpianto]
					if ok {
						nome = station.Nome
						tipo = string(station.Tipo)
						comune = station.Comune
						provincia = station.Provincia
						bandiera = station.Bandiera
					}
					carburantiGauge.WithLabelValues(
						strconv.FormatInt(int64(record.IDImpianto), 10), // IDImpianto
						record.Carburante,                      // Carburante
						strconv.FormatBool(record.SelfService), // SelfService
						nome,                                   // Nome
						tipo,                                   // Tipo
						comune,                                 // Comune
						provincia,                              // Provincia
						bandiera,                               // Bandiera
					).Set(record.Prezzo)
				}
			}
		break_loop:
			log.Printf("Sleeping for %s", *flagSleepInterval)
			time.Sleep(*flagSleepInterval)
		}
	}()

	http.Handle(*flagPath, promhttp.Handler())
	log.Printf("Starting server on %s", *flagListen)
	log.Fatal(http.ListenAndServe(*flagListen, nil))
}

type Record struct {
	IDImpianto        int
	Carburante        string
	Prezzo            float64
	SelfService       bool
	DataComunicazione time.Time
}

func refreshRecords(cache *Cache) ([]*Record, error) {
	resp, err := http.Get(pricesCSVURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch prices: %w", err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	// skip the first two lines. This is a non-compliant CSV with a two-line
	// header.
	for i := 0; i < 2; i++ {
		if _, _, err := br.ReadLine(); err != nil {
			return nil, fmt.Errorf("failed to read line: %w", err)
		}
	}
	r := csv.NewReader(br)
	r.Comma = ';'
	r.FieldsPerRecord = 5
	var records []*Record
	for {
		items, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV record: %w", err)
		}
		record, err := parseRecord(items)
		if err != nil {
			return nil, fmt.Errorf("failed to parse record: %w", err)
		}
		records = append(records, record)
		k := fmt.Sprintf("%d-%d", record.IDImpianto, record.DataComunicazione.Unix())
		cache.Put(k, *record)
	}
	return records, nil
}

func parseRecord(items []string) (*Record, error) {
	if len(items) != 5 {
		return nil, fmt.Errorf("expected 5 fields, got %d", len(items))
	}
	var r Record

	idImpianto, err := strconv.ParseInt(items[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("IDImpianto is not a numeric string: %w", err)
	}
	r.IDImpianto = int(idImpianto)
	r.Carburante = items[1]
	r.Prezzo, err = strconv.ParseFloat(items[2], 64)
	if err != nil {
		return nil, fmt.Errorf("Prezzo is not a float string: %w", err)
	}
	r.SelfService, err = strconv.ParseBool(items[3])
	if err != nil {
		return nil, fmt.Errorf("SelfService is not a bool string: %w", err)
	}
	r.DataComunicazione, err = time.Parse("2/1/2006 15:04:05", items[4])
	if err != nil {
		return nil, fmt.Errorf("DataComunicazione is not a time string: %w", err)
	}

	return &r, nil
}

type Station struct {
	ID        int
	Gestore   string
	Bandiera  string
	Tipo      StationType
	Nome      string
	Indirizzo string
	Comune    string
	Provincia string
	Lat       string
	Long      string
}

type StationType string

const (
	StationTypeStradale     = "Stradale"
	StationTypeAutostradale = "Autostradale"
)

func updateStations() (map[int]Station, error) {
	resp, err := http.Get(stationsCSVURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch station data: %w", err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	// skip the first two lines. This is a non-compliant CSV with a two-line
	// header.
	for i := 0; i < 2; i++ {
		if _, _, err := br.ReadLine(); err != nil {
			return nil, fmt.Errorf("failed to read line: %w", err)
		}
	}
	r := csv.NewReader(br)
	r.Comma = ';'
	r.FieldsPerRecord = 10
	r.LazyQuotes = true
	stationMap := make(map[int]Station)
	for {
		items, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			if errors.Is(err, &csv.ParseError{}) {
				if strings.Contains(err.Error(), "wrong number of fields") {
					log.Printf("Skipping malformed record with wrong number of fields")
					continue
				}
			}
		}
		idImpianto, err := strconv.ParseInt(items[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("IDImpianto is not a numeric string: %w", err)
		}
		_, ok := stationMap[int(idImpianto)]
		if ok {
			log.Printf("Found duplicate type '%s' for station ID %d", items[3], idImpianto)
		}
		stationMap[int(idImpianto)] = Station{
			ID:        int(idImpianto),
			Gestore:   items[1],
			Bandiera:  items[2],
			Tipo:      StationType(items[3]),
			Nome:      items[4],
			Indirizzo: items[5],
			Comune:    items[6],
			Provincia: items[7],
			Lat:       items[8],
			Long:      items[9],
		}
	}
	return stationMap, nil
}
