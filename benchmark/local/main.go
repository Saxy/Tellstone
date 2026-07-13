package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/components"
	"github.com/go-echarts/go-echarts/v2/opts"
)

type MemtierResult struct {
	AllStats struct {
		Sets struct {
			OpsPerSec   float64 `json:"Ops/sec"`
			AvgLatency  float64 `json:"Average Latency"`
			Percentiles struct {
				P50  float64 `json:"p50.00"`
				P99  float64 `json:"p99.00"`
				P999 float64 `json:"p99.90"`
			} `json:"Percentile Latencies"`
		} `json:"Sets"`
		Gets struct {
			OpsPerSec   float64 `json:"Ops/sec"`
			AvgLatency  float64 `json:"Average Latency"`
			Percentiles struct {
				P50  float64 `json:"p50.00"`
				P99  float64 `json:"p99.00"`
				P999 float64 `json:"p99.90"`
			} `json:"Percentile Latencies"`
		} `json:"Gets"`
		Totals struct {
			OpsPerSec  float64 `json:"Ops/sec"`
			AvgLatency float64 `json:"Average Latency"`
			KBPerSec   float64 `json:"KB/sec"`
		} `json:"Totals"`
	} `json:"ALL STATS"`
}

type BenchData struct {
	Engine string
	CPUs   int
	Result MemtierResult
}

var engineColors = map[string]string{
	"redis":     "#DC382C",
	"valkey":    "#FF6B35",
	"dragonfly": "#1E90FF",
	"tellstone": "#2ECC71",
}

func main() {
	engines := []string{"redis", "valkey", "dragonfly", "tellstone"}
	cpuCounts := []int{4, 16, 32}
	dataDir := "."

	var allData []BenchData

	for _, engine := range engines {
		for _, cpus := range cpuCounts {
			fileName := fmt.Sprintf("%s_%dc_bench.json", engine, cpus)
			filePath := filepath.Join(dataDir, fileName)

			file, err := os.Open(filePath)
			if err != nil {
				fmt.Printf("[MISSING] %s\n", filePath)
				continue
			}
			byteValue, _ := io.ReadAll(file)
			file.Close()

			var result MemtierResult
			if err := json.Unmarshal(byteValue, &result); err != nil {
				fmt.Printf("[ERROR] %s: %v\n", filePath, err)
				continue
			}

			fmt.Printf("[OK] %s -> Total Ops/sec: %.0f | SET Ops/sec: %.0f | GET Ops/sec: %.0f | SET p50: %.2fms p99: %.2fms | GET p50: %.2fms p99: %.2fms\n",
				fileName,
				result.AllStats.Totals.OpsPerSec,
				result.AllStats.Sets.OpsPerSec,
				result.AllStats.Gets.OpsPerSec,
				result.AllStats.Sets.Percentiles.P50,
				result.AllStats.Sets.Percentiles.P99,
				result.AllStats.Gets.Percentiles.P50,
				result.AllStats.Gets.Percentiles.P99,
			)

			allData = append(allData, BenchData{Engine: engine, CPUs: cpus, Result: result})
		}
	}

	if len(allData) == 0 {
		fmt.Println("No benchmark data found. Run benchmarks first.")
		return
	}

	page := components.NewPage()
	page.PageTitle = "Tellstone Benchmark Suite – Redis vs Valkey vs Dragonfly vs Tellstone"
	page.AddCharts(
		makeOpsBarChart("Total Ops/sec", cpuCounts, allData, func(d BenchData) float64 { return d.Result.AllStats.Totals.OpsPerSec }),
		makeOpsBarChart("SET Ops/sec", cpuCounts, allData, func(d BenchData) float64 { return d.Result.AllStats.Sets.OpsPerSec }),
		makeOpsBarChart("GET Ops/sec", cpuCounts, allData, func(d BenchData) float64 { return d.Result.AllStats.Gets.OpsPerSec }),
		makeLatencyBarChart("SET Avg Latency (ms)", cpuCounts, allData, func(d BenchData) float64 { return d.Result.AllStats.Sets.AvgLatency }),
		makeLatencyBarChart("GET Avg Latency (ms)", cpuCounts, allData, func(d BenchData) float64 { return d.Result.AllStats.Gets.AvgLatency }),
		makeLatencyBarChart("SET p99 Latency (ms)", cpuCounts, allData, func(d BenchData) float64 { return d.Result.AllStats.Sets.Percentiles.P99 }),
		makeLatencyBarChart("GET p99 Latency (ms)", cpuCounts, allData, func(d BenchData) float64 { return d.Result.AllStats.Gets.Percentiles.P99 }),
		makeLatencyBarChart("SET p99.9 Latency (ms)", cpuCounts, allData, func(d BenchData) float64 { return d.Result.AllStats.Sets.Percentiles.P999 }),
		makeLatencyBarChart("GET p99.9 Latency (ms)", cpuCounts, allData, func(d BenchData) float64 { return d.Result.AllStats.Gets.Percentiles.P999 }),
		makeThroughputBarChart("Throughput (KB/sec)", cpuCounts, allData),
	)

	f, err := os.Create("benchmark_results.html")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if err := page.Render(f); err != nil {
		panic(err)
	}
	fmt.Println("\nGenerated: benchmark_results.html")
}

func getEnginesWithData(cpuCounts []int, allData []BenchData) []string {
	seen := map[string]bool{}
	var result []string
	for _, cpu := range cpuCounts {
		for _, d := range allData {
			if d.CPUs == cpu && !seen[d.Engine] {
				seen[d.Engine] = true
				result = append(result, d.Engine)
			}
		}
	}
	sort.Strings(result)
	return result
}

func makeOpsBarChart(title string, cpuCounts []int, allData []BenchData, extract func(BenchData) float64) *charts.Bar {
	engines := getEnginesWithData(cpuCounts, allData)
	xAxis := make([]string, len(cpuCounts))
	for i, c := range cpuCounts {
		xAxis[i] = fmt.Sprintf("%d CPUs", c)
	}

	bar := charts.NewBar()
	bar.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{Title: title}),
		charts.WithTooltipOpts(opts.Tooltip{Show: opts.Bool(true)}),
		charts.WithLegendOpts(opts.Legend{Show: opts.Bool(true), Right: "5%"}),
		charts.WithYAxisOpts(opts.YAxis{Name: "Ops/sec", SplitLine: &opts.SplitLine{Show: opts.Bool(true)}}),
		charts.WithInitializationOpts(opts.Initialization{
			Theme:  "walden",
			Width:  "1200px",
			Height: "500px",
		}),
	)

	bar.SetXAxis(xAxis)

	for _, engine := range engines {
		var items []opts.BarData
		for _, cpu := range cpuCounts {
			val := 0.0
			for _, d := range allData {
				if d.Engine == engine && d.CPUs == cpu {
					val = extract(d)
					break
				}
			}
			items = append(items, opts.BarData{Value: val})
		}
		bar.AddSeries(engine, items)
	}

	return bar
}

func makeLatencyBarChart(title string, cpuCounts []int, allData []BenchData, extract func(BenchData) float64) *charts.Bar {
	engines := getEnginesWithData(cpuCounts, allData)
	xAxis := make([]string, len(cpuCounts))
	for i, c := range cpuCounts {
		xAxis[i] = fmt.Sprintf("%d CPUs", c)
	}

	bar := charts.NewBar()
	bar.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{Title: title}),
		charts.WithTooltipOpts(opts.Tooltip{Show: opts.Bool(true)}),
		charts.WithLegendOpts(opts.Legend{Show: opts.Bool(true), Right: "5%"}),
		charts.WithYAxisOpts(opts.YAxis{Name: "Milliseconds", SplitLine: &opts.SplitLine{Show: opts.Bool(true)}}),
		charts.WithInitializationOpts(opts.Initialization{
			Theme:  "walden",
			Width:  "1200px",
			Height: "500px",
		}),
	)

	bar.SetXAxis(xAxis)

	for _, engine := range engines {
		var items []opts.BarData
		for _, cpu := range cpuCounts {
			val := 0.0
			for _, d := range allData {
				if d.Engine == engine && d.CPUs == cpu {
					val = extract(d)
					break
				}
			}
			items = append(items, opts.BarData{Value: fmt.Sprintf("%.3f", val)})
		}
		bar.AddSeries(engine, items)
	}

	return bar
}

func makeThroughputBarChart(title string, cpuCounts []int, allData []BenchData) *charts.Bar {
	engines := getEnginesWithData(cpuCounts, allData)
	xAxis := make([]string, len(cpuCounts))
	for i, c := range cpuCounts {
		xAxis[i] = fmt.Sprintf("%d CPUs", c)
	}

	bar := charts.NewBar()
	bar.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{Title: title}),
		charts.WithTooltipOpts(opts.Tooltip{Show: opts.Bool(true)}),
		charts.WithLegendOpts(opts.Legend{Show: opts.Bool(true), Right: "5%"}),
		charts.WithYAxisOpts(opts.YAxis{Name: "KB/sec", SplitLine: &opts.SplitLine{Show: opts.Bool(true)}}),
		charts.WithInitializationOpts(opts.Initialization{
			Theme:  "walden",
			Width:  "1200px",
			Height: "500px",
		}),
	)

	bar.SetXAxis(xAxis)

	for _, engine := range engines {
		var items []opts.BarData
		for _, cpu := range cpuCounts {
			val := 0.0
			for _, d := range allData {
				if d.Engine == engine && d.CPUs == cpu {
					val = d.Result.AllStats.Totals.KBPerSec
					break
				}
			}
			items = append(items, opts.BarData{Value: val})
		}
		bar.AddSeries(engine, items)
	}

	return bar
}

func makeLatencyComparisonBarChart(title string, cpuCounts []int, allData []BenchData) *charts.Bar {
	engines := getEnginesWithData(cpuCounts, allData)
	xAxis := make([]string, len(cpuCounts))
	for i, c := range cpuCounts {
		xAxis[i] = fmt.Sprintf("%d CPUs", c)
	}

	bar := charts.NewBar()
	bar.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{Title: title}),
		charts.WithTooltipOpts(opts.Tooltip{Show: opts.Bool(true)}),
		charts.WithLegendOpts(opts.Legend{Show: opts.Bool(true), Right: "5%"}),
		charts.WithYAxisOpts(opts.YAxis{Name: "Milliseconds", SplitLine: &opts.SplitLine{Show: opts.Bool(true)}}),
		charts.WithInitializationOpts(opts.Initialization{
			Theme:  "walden",
			Width:  "1200px",
			Height: "500px",
		}),
	)

	bar.SetXAxis(xAxis)

	for _, engine := range engines {
		var items []opts.BarData
		for _, cpu := range cpuCounts {
			val := 0.0
			for _, d := range allData {
				if d.Engine == engine && d.CPUs == cpu {
					val = d.Result.AllStats.Totals.AvgLatency
					break
				}
			}
			items = append(items, opts.BarData{Value: fmt.Sprintf("%.3f", val)})
		}
		bar.AddSeries(engine, items)
	}

	return bar
}


