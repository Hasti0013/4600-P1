# Project 1: Process Scheduler

## Description

The University of North Texas' CSCE 4600 course includes this project. In this project, I'm developing a straightforward process scheduler that reads a file containing sample processes and generates a schedule using one of three distinct schedule types:

- First Come First Serve (FCFS)
- Shortest Job First (SJF)
- SJF Priority
- Round-robin (RR) with Time Quantum equals to 1

Assuming that all processes are CPU bound (they do not block for I/O).
## Steps

1. Clone down the example input/output and skeleton main.go:

   1. `git clone https://github.com/Hasti0013/CSCE4600`

 To run using the example processes, type into the command line:
   `go run . example_processes.csv`
