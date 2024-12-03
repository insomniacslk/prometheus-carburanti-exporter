package main

import (
	"bufio"
	"encoding/csv"
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
	log.Printf("Updating stations from %q", stationsCSVURL)
	resp, err := http.Get(stationsCSVURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch station data: %w", err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	// skip the first two lines. This is a non-compliant CSV with a two-line
	// header.
	stationMap := make(map[int]Station)
	scanner := bufio.NewScanner(br)
	lineno := 1
	for scanner.Scan() {
		// cannot use the csv package because the input CSV is malformed (unterminated quotes)
		// and the csv package doesn't deal with that.
		if lineno == 1 {
			// skip header
			continue
		}
		line := scanner.Text()
		items := strings.Split(line, ";")
		if len(items) == 0 {
			log.Printf("Warning: skipping empty line")
			continue
		}
		idImpianto, err := strconv.ParseInt(items[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("IDImpianto is not a numeric string: %w", err)
		}
		_, ok := stationMap[int(idImpianto)]
		if ok {
			log.Printf("Warning: found duplicate type '%s' for station ID %d, using the latest value", items[3], idImpianto)
		}
		address := ""
		switch len(items) {
		case 10:
			address = items[5]
		case 11:
			// there is a bug in the data source, where the items can be 11 instead of 10.
			// The extra field is a second version of the address, so we concatenate it to
			// `Indirizzo`.
			address = strings.Join(items[5:6], " | ")
		default:
			return nil, fmt.Errorf("malformed line with %d fields instead of 10 or 11: %q", len(items), items)
		}
		stationMap[int(idImpianto)] = Station{
			ID:        int(idImpianto),
			Gestore:   items[1],
			Bandiera:  items[2],
			Tipo:      StationType(items[3]),
			Nome:      items[4],
			Indirizzo: address,
			Comune:    items[6],
			Provincia: items[7],
			Lat:       items[8],
			Long:      items[9],
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan stations CSV: %w", err)
	}
	return stationMap, nil
}
