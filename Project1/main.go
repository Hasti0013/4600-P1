package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"github.com/olekukonko/tablewriter"
	"math"
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

	// Shortest job first scheduling
	SJFSchedule(os.Stdout, "Shortest-job-first", processes)
	
	// Priority scheduling
	SJFPrioritySchedule(os.Stdout, "Priority", processes)
	
	// Round robin scheduling
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
		serviceTime     int64
		minPriority 	int64
		lastStart		int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     = make([]int64, len(processes))
		turnAroundTime  = make([]int64, len(processes))
		remTime			= make([]int64, len(processes))
		completion		= make([]int64, len(processes))  
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	completed := 0
	minPriority = math.MaxInt64 // Tracks the value of the lowest priority
	priority := 0 // Tracks the index of the process with the lowest priority
	lastPriority := 0 // Tracks the index of priority of the previous iteration
	check := false
	count := len(processes)
	
	for i := range processes { // Populating the remaining time array that will be updated along the way
		remTime[i] = processes[i].BurstDuration
	}

	for completed != count {
		for j := 0; j < count; j++ {
			if processes[j].ArrivalTime <= serviceTime && processes[j].Priority < minPriority && remTime[j] > 0 {
				minPriority = processes[j].Priority
				priority = j
				check = true
			}
		}

		// Every preemption, update Gantt schedule with the preempted process
		if priority != lastPriority {
			gantt = append(gantt, TimeSlice{
				PID:   processes[lastPriority].ProcessID,
				Start: lastStart,
				Stop:  serviceTime,
			})
			lastStart = serviceTime
			lastPriority = priority
		}

		if check == false {
			serviceTime++
			continue
		}

		remTime[priority]--

		if remTime[priority] == 0 {
			completed++
			check = false
			completion[priority] = serviceTime + 1
			lastCompletion = float64(completion[priority])
			waitingTime[priority] = completion[priority] - processes[priority].BurstDuration - processes[priority].ArrivalTime
			if waitingTime[priority] < 0 {
				waitingTime[priority] = 0
			}
			minPriority = math.MaxInt64
		}

		serviceTime++
	}

	for i := range waitingTime {
		totalWait += float64(waitingTime[i])
		turnAroundTime[i] = processes[i].BurstDuration + waitingTime[i]
		totalTurnaround += float64(turnAroundTime[i])
	}
	aveWait := totalWait / float64(count)
	aveTurnaround := totalTurnaround / float64(count)
	aveThroughput := float64(count) / lastCompletion

	for i := range processes {
		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime[i]),
			fmt.Sprint(turnAroundTime[i]),
			fmt.Sprint(completion[i]),
		}
	}

	// Adding the last entry of the Gantt schedule
	gantt = append(gantt, TimeSlice{
		PID:   processes[lastPriority].ProcessID,
		Start: lastStart,
		Stop:  serviceTime,
	})

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		minTime 		int64
		lastStart		int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     = make([]int64, len(processes))
		turnAroundTime  = make([]int64, len(processes))
		remTime			= make([]int64, len(processes))
		completion		= make([]int64, len(processes))  
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	completed := 0
	minTime = math.MaxInt64
	shortest := 0
	lastShortest := 0
	check := false
	count := len(processes)
	
	for i := range processes { // Populating the remaining time array that will be updated along the way
		remTime[i] = processes[i].BurstDuration
	}

	for completed != count {
		for j := 0; j < count; j++ {
			if processes[j].ArrivalTime <= serviceTime && remTime[j] < minTime && remTime[j] > 0 {
				minTime = remTime[j]
				shortest = j
				check = true
			}
		}

		// Every preemption, update Gantt schedule with the preempted process
		if shortest != lastShortest {
			gantt = append(gantt, TimeSlice{
				PID:   processes[lastShortest].ProcessID,
				Start: lastStart,
				Stop:  serviceTime,
			})
			lastStart = serviceTime
			lastShortest = shortest
		}

		if check == false {
			serviceTime++
			continue
		}

		remTime[shortest]--

		minTime = remTime[shortest]
		if (minTime == 0) {
			minTime = math.MaxInt64
		}

		if remTime[shortest] == 0 {
			completed++
			check = false
			completion[shortest] = serviceTime + 1
			lastCompletion = float64(completion[shortest])
			waitingTime[shortest] = completion[shortest] - processes[shortest].BurstDuration - processes[shortest].ArrivalTime
			if waitingTime[shortest] < 0 {
				waitingTime[shortest] = 0
			}
		}

		serviceTime++
	}

	for i := range waitingTime {
		totalWait += float64(waitingTime[i])
		turnAroundTime[i] = processes[i].BurstDuration + waitingTime[i]
		totalTurnaround += float64(turnAroundTime[i])
	}
	aveWait := totalWait / float64(count)
	aveTurnaround := totalTurnaround / float64(count)
	aveThroughput := float64(count) / lastCompletion

	for i := range processes {
		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime[i]),
			fmt.Sprint(turnAroundTime[i]),
			fmt.Sprint(completion[i]),
		}
	}

	// Adding the last entry of the Gantt schedule
	gantt = append(gantt, TimeSlice{
		PID:   processes[lastShortest].ProcessID,
		Start: lastStart,
		Stop:  serviceTime,
	})

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func RRSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		lastStart		int64
		timeQuantum		int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     = make([]int64, len(processes))
		turnAroundTime  = make([]int64, len(processes))
		remTime			= make([]int64, len(processes))
		completion		= make([]int64, len(processes))  
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	timeQuantum = 1
	completed := 0
	count := len(processes)
	turn := 0
	check := false // boolean to check if we're trying to find the next available process
	stuck := 0 // variable that tracks the stuck process
	
	for i := range processes { // Populating the remaining time array that will be updated along the way
		remTime[i] = processes[i].BurstDuration
	}

	for completed != count {
		if processes[turn].ArrivalTime > serviceTime || remTime[turn] == 0 {
			turn = (turn + 1) % count
			if check == false { // encountering invalid process for the first time
				check = true
				stuck = turn
			} else if stuck == turn { // meeting the invalid process that we were stuck with the first time
				serviceTime++
				lastStart = serviceTime 
				check = false
				turn = 0
			}
			continue
		}
		check = false // found a process that's valid to process
		if remTime[turn] > timeQuantum {
			serviceTime += timeQuantum
			remTime[turn] -= timeQuantum 
		} else {
			serviceTime += remTime[turn]
			remTime[turn] = 0
			completed++
			completion[turn] = serviceTime
			lastCompletion = float64(completion[turn])
			waitingTime[turn] = completion[turn] - processes[turn].BurstDuration - processes[turn].ArrivalTime
			if waitingTime[turn] < 0 {
				waitingTime[turn] = 0
			}	
		}
		gantt = append(gantt, TimeSlice{
			PID:   processes[turn].ProcessID,
			Start: lastStart,
			Stop:  serviceTime,
		})
		lastStart = serviceTime
		turn = (turn + 1) % count
	}

	for i := range waitingTime {
		totalWait += float64(waitingTime[i])
		turnAroundTime[i] = processes[i].BurstDuration + waitingTime[i]
		totalTurnaround += float64(turnAroundTime[i])
	}
	aveWait := totalWait / float64(count)
	aveTurnaround := totalTurnaround / float64(count)
	aveThroughput := float64(count) / lastCompletion

	for i := range processes {
		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime[i]),
			fmt.Sprint(turnAroundTime[i]),
			fmt.Sprint(completion[i]),
		}
	}

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)	
}

//endregion

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
