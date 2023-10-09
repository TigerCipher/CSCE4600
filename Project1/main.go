package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)


func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	SJFSchedule(os.Stdout, "Shortest-job-first", processes)
	//
	SJFPrioritySchedule(os.Stdout, "Priority", processes)
	//
	RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

type ByBurstDuration []Process

func (a ByBurstDuration) Len() int           { return len(a) }
func (a ByBurstDuration) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByBurstDuration) Less(i, j int) bool { return a[i].BurstDuration < a[j].BurstDuration }

type ByPriority []Process

func (a ByPriority) Len() int           { return len(a) }
func (a ByPriority) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByPriority) Less(i, j int) bool { return a[i].Priority > a[j].Priority }

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFPrioritySchedule(w io.Writer, title string, processes []Process) { 
	var (
        serviceTime int64
        totalWait   float64
        totalTurnaround float64
        lastCompletion float64
        schedule    = make([][]string, len(processes))
        gantt       = make([]TimeSlice, 0)
    )
    remaining := make([]Process, len(processes))
    copy(remaining, processes)

	byBurstTime := func(i, j int) bool {
		return remaining[i].Priority > remaining[j].Priority
	}


    sort.SliceStable(remaining, byBurstTime)

    for len(remaining) > 0 {
        next := findShortestJobWithPriority(remaining, serviceTime)
        if next == nil {
            // No available jobs
            serviceTime++
            continue
        }
        process := *next
        remaining = removeProcess(remaining, process)

        waitingTime := serviceTime - process.ArrivalTime
        if waitingTime < 0 {
            waitingTime = 0
        }
        totalWait += float64(waitingTime)

        start := serviceTime

        turnaround := process.BurstDuration + waitingTime
        totalTurnaround += float64(turnaround)

        completion := process.BurstDuration + serviceTime
        lastCompletion = float64(completion)

        schedule[process.ProcessID-1] = []string{
            fmt.Sprint(process.ProcessID),
            fmt.Sprint(process.Priority),
            fmt.Sprint(process.BurstDuration),
            fmt.Sprint(process.ArrivalTime),
            fmt.Sprint(waitingTime),
            fmt.Sprint(turnaround),
            fmt.Sprint(completion),
        }

        gantt = append(gantt, TimeSlice{
            PID:   process.ProcessID,
            Start: start,
            Stop:  completion,
        })

        serviceTime += process.BurstDuration
    }

    count := float64(len(processes))
    aveWait := totalWait / count
    aveTurnaround := totalTurnaround / count
    aveThroughput := count / lastCompletion

    outputTitle(w, title)
    outputGantt(w, gantt)
    outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}
//
func SJFSchedule(w io.Writer, title string, processes []Process) { 
	var (
        serviceTime int64
        totalWait   float64
        totalTurnaround float64
        lastCompletion float64
		completed int64
		min int64
		shortest int
		check bool
		lastId int = -1
        schedule    = make([][]string, len(processes))
        gantt       = make([]TimeSlice, 0)
		waitingTimes = make([]int64, len(processes))
		completions = make([]int64, len(processes))
    )

	remainingBursts := make([]int64, len(processes))
	for i := range processes {
		remainingBursts[i] = processes[i].BurstDuration
	}
	min = 99999
	n := int64(len(processes))

	for completed != n {
		start := serviceTime

		for i := range processes {
			if (processes[i].ArrivalTime <= serviceTime && remainingBursts[i] < min) && remainingBursts[i] > 0 {
				min = remainingBursts[i]
				shortest = i
				check = true
			}
		}

		if !check {
			serviceTime++
			continue
		}

		remainingBursts[shortest]--
		min = remainingBursts[shortest]
		if min == 0 {
			min = 9999
		}

		if remainingBursts[shortest] == 0 {
			completed++
			check = false
			lastCompletion = float64(serviceTime) + 1
			completions[shortest] = int64(lastCompletion)
			waitingTimes[shortest] = int64(lastCompletion) - processes[shortest].BurstDuration - processes[shortest].ArrivalTime

			if waitingTimes[shortest] < 0 {
				waitingTimes[shortest] = 0
			}
			totalWait += float64(waitingTimes[shortest])
			gantt = append(gantt, TimeSlice{
				PID:   processes[shortest].ProcessID,
				Start: start,
				Stop:  int64(lastCompletion),
			})
		}

		serviceTime++
		if lastId != shortest {
			lastId = shortest
			gantt = append(gantt, TimeSlice{
				PID:   processes[shortest].ProcessID,
				Start: start,
				Stop:  serviceTime,
			})
		}
	}

	for i := range processes {
		turnaround := processes[i].BurstDuration + waitingTimes[i]
		totalTurnaround += float64(turnaround)
		schedule[processes[i].ProcessID-1] = []string{
            fmt.Sprint(processes[i].ProcessID),
            fmt.Sprint(processes[i].Priority),
            fmt.Sprint(processes[i].BurstDuration),
            fmt.Sprint(processes[i].ArrivalTime),
            fmt.Sprint(waitingTimes[i]),
            fmt.Sprint(turnaround),
            fmt.Sprint(completions[i]),
        }
	}

    count := float64(len(processes))
    aveWait := totalWait / count
    aveTurnaround := totalTurnaround / count
    aveThroughput := count / lastCompletion

    outputTitle(w, title)
    outputGantt(w, gantt)
    outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}
//
func RRSchedule(w io.Writer, title string, processes []Process) { 
	var (
		serviceTime int64
        totalWait   float64
        totalTurnaround float64
        lastCompletion int64
		done bool = false
		quantum int64 = 3
        schedule    = make([][]string, len(processes))
        gantt       = make([]TimeSlice, 0)
		waitingTimes = make([]int64, len(processes))
		remainingBursts = make([]int64, len(processes))
		completions = make([]int64, len(processes))
		turnarounds = make([]int64, len(processes))
    )

	for i := range processes {
		remainingBursts[i] = processes[i].BurstDuration
	}

	for !done {
		done = true
		for i := range processes {
			if remainingBursts[i] > 0 {
				done = false
				if remainingBursts[i] > quantum {
					completions[i] += quantum
					serviceTime += quantum
					remainingBursts[i] -= quantum
					
					gantt = append(gantt, TimeSlice{
						PID:   processes[i].ProcessID,
						Start: lastCompletion,
						Stop:  serviceTime,
					})

					lastCompletion = serviceTime
				} else {
					serviceTime = serviceTime + remainingBursts[i]
					completions[i] = serviceTime
					remainingBursts[i] = 0
					gantt = append(gantt, TimeSlice{
						PID:   processes[i].ProcessID,
						Start: lastCompletion,
						Stop:  serviceTime,
					})
				}
			}
		}
	}

	serviceTime += quantum
	for i := range processes {
		turnarounds[i] = completions[i] - processes[i].ArrivalTime
		waitingTimes[i] = turnarounds[i] - processes[i].BurstDuration
		schedule[processes[i].ProcessID-1] = []string{
            fmt.Sprint(processes[i].ProcessID),
            fmt.Sprint(processes[i].Priority),
            fmt.Sprint(processes[i].BurstDuration),
            fmt.Sprint(processes[i].ArrivalTime),
            fmt.Sprint(waitingTimes[i]),
            fmt.Sprint(turnarounds[i]),
            fmt.Sprint(completions[i]),
        }

		totalWait += float64(waitingTimes[i])
		totalTurnaround += float64(turnarounds[i])
	}

	count := float64(len(processes))
    aveWait := totalWait / count
    aveTurnaround := totalTurnaround / count
    aveThroughput := count / float64(serviceTime)

    outputTitle(w, title)
    outputGantt(w, gantt)
    outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

//endregion


func findShortestJob(remaining []Process, serviceTime int64) *Process {
    var shortest *Process
    for i := range remaining {
        if remaining[i].ArrivalTime > serviceTime {
            break
        }
        if shortest == nil || remaining[i].BurstDuration < shortest.BurstDuration {
            shortest = &remaining[i]
        }
    }
    return shortest
}

func findShortestJobWithPriority(remaining []Process, serviceTime int64) *Process {
	var shortest *Process
	for i := range remaining {
		if remaining[i].ArrivalTime > serviceTime {
			break
		}
		if shortest == nil || remaining[i].Priority > shortest.Priority {
			shortest = &remaining[i]
		}
	}
	return shortest
}

func removeProcess(processes []Process, process Process) []Process {
    var remaining []Process
    for i := range processes {
        if processes[i].ProcessID != process.ProcessID {
            remaining = append(remaining, processes[i])
        }
    }
    return remaining
}

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
