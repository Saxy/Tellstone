package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
)

// Erweitertes JSON-Struct mit Fallbacks für memtier_benchmark-Strukturvariationen
type MemtierJSON struct {
	AllStats struct {
		Totals struct {
			OpsPerSec float64 `json:"Ops/sec"`
		} `json:"Totals"`
	} `json:"ALL STATS"`
}

func main() {
	profiles := []string{"4_16", "16_64", "60_128"}
	engines := []string{"redis", "valkey", "dragonfly", "tellstone"}

	seriesData := make(map[string][]opts.BarData)
	for _, engine := range engines {
		seriesData[engine] = make([]opts.BarData, 0)
	}

	fmt.Println("=== Checking JSON Files and Values ===")

	for _, profile := range profiles {
		// Fix: Zuverlässiges Splitten statt fehleranfälligem Sscanf
		parts := strings.Split(profile, "_")
		if len(parts) != 2 {
			fmt.Printf("[ERROR] Invalid profile format: %s\n", profile)
			continue
		}
		threads, clients := parts[0], parts[1]

		for _, engine := range engines {
			fileName := fmt.Sprintf("%s_%st_%sc.json", engine, threads, clients)
			filePath := filepath.Join(".", fileName)

			ops := 0.0
			file, err := os.Open(filePath)
			if err != nil {
				fmt.Printf("[MISSING] File not found: %s\n", filePath)
				seriesData[engine] = append(seriesData[engine], opts.BarData{Value: 0})
				continue
			}

			byteValue, _ := io.ReadAll(file)
			file.Close()

			var result MemtierJSON
			if err := json.Unmarshal(byteValue, &result); err != nil {
				fmt.Printf("[JSON ERROR] Could not unmarshal %s: %v\n", filePath, err)
			} else {
				ops = result.AllStats.Totals.OpsPerSec
				// Debug-Ausgabe auf dem Terminal
				fmt.Printf("[FOUND] %s -> Ops/sec: %.2f\n", filePath, ops)
			}

			seriesData[engine] = append(seriesData[engine], opts.BarData{Value: ops})
		}
	}

	fmt.Println("\n=== Generating Chart ===")
	bar := charts.NewBar()
	bar.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{
			Title:    "Database Benchmark Comparison",
			Subtitle: "Throughput (Operations per Second - Higher is Better)",
		}),
		charts.WithTooltipOpts(opts.Tooltip{Show: opts.Bool(true)}),
		charts.WithLegendOpts(opts.Legend{Show: opts.Bool(true), Right: "10%"}),
	)

	// X-Achse setzen
	bar.SetXAxis(profiles)

	// Datenreihen hinzufügen
	for _, engine := range engines {
		bar.AddSeries(engine, seriesData[engine])
	}

	f, err := os.Create("benchmark_results.html")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if err := bar.Render(f); err != nil {
		panic(err)
	}
	fmt.Println("Generated chart output: benchmark_results.html")
}
