package main

import (
	"flag"
	"log"
	"os"
	"os/exec"
	"bytes"
	"strings"
	"sync"
	"time"
	"io/ioutil"
	"path"
	"fmt"
	"regexp"
	"strconv"
	"sort"
	"runtime/pprof"
	"os/signal"
	"syscall"
)

type result struct {
	date     string
	lines    int
}

func main() {
	numWorkers := flag.Int("num-workers", 1, "number of worker goroutines")
	tmpDir := flag.String("tmp", os.TempDir(), "directory for temporary files")
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	flag.Parse()
	if flag.NArg() != 1 {
		log.Fatalf("Usage: %s <path to git dir>", os.Args[0])
	}
	gitDir := flag.Arg(0)

	f, err := os.Create(*cpuprofile)
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	revisions, err := revisions(gitDir)
	if err != nil {
		log.Fatal(err)
	}
	numRevisions := len(revisions)

	revisionChan := make(chan string)
	resultChan := make(chan result)

	log.Printf("Starting workers...")
	var workerWg sync.WaitGroup
	for i := 0; i < *numWorkers; i++ {
		workerWg.Add(1)
		go worker(gitDir, *tmpDir, resultChan, revisionChan, &workerWg)
	}

	var wgAggregator sync.WaitGroup
	wgAggregator.Add(1)
	go aggregator(numRevisions, resultChan, &wgAggregator)

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT)

	go func() {
		defer close(revisionChan)
		defer log.Printf("Shutting down producer")

		i := 0
		for {
			select {
			case sig := <-sigChan:
				if sig == syscall.SIGINT {
					log.Printf("SIGINT")
					return
				}
			case revisionChan <- revisions[i]:
				i++
				if i == numRevisions {
					return
				}
			}
		}
	}()

	workerWg.Wait()
	log.Printf("All workers done. Closing resultChan.")

	close(resultChan)
	log.Printf("Waiting for aggregator")
	wgAggregator.Wait()
}

func aggregator(numRevisions int, resultChan <-chan result, wg *sync.WaitGroup) {
	defer wg.Done()
	defer log.Printf("Shutting down aggregator")

	log.Printf("Starting aggregator")

	start := time.Now()
	histogram := make(map[string]int)
	processedResults := 0

	for result := range resultChan {
		processedResults++
		if processedResults%100 == 0 {
			shareDone := float64(processedResults) / float64(numRevisions)
			eta := time.Duration(time.Duration(time.Since(start).Seconds()/shareDone) * time.Second)
			log.Printf("%.2f%% done, eta %v", shareDone*100, eta)
		}

		if _, ok := histogram[result.date]; ok {
			if histogram[result.date] < result.lines {
				histogram[result.date] = result.lines
			}
		} else {
			histogram[result.date] = result.lines
		}
	}

	var dates []string
	for date := range histogram {
		dates = append(dates, date)
	}

	sort.Strings(dates)

	for _, date := range dates {
		ts, err := time.Parse("2006-01-02", date)
		if err != nil {
			log.Fatalf("unable to parse date: %v", err)
		}
		fmt.Printf("%d,%d\n", ts.Unix(), histogram[date])
	}

	log.Printf("total time: %v", time.Since(start))
}

func revisions(gitDir string) ([]string, error) {
	var buf bytes.Buffer
	cmd := exec.Command("git", "--git-dir="+gitDir, "log", "--pretty=format:%H")
	cmd.Stdout = &buf
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("git log failed: %v", err)
	}

	return strings.Split(buf.String(), "\n"), nil
}

func worker(gitDir, tmpDir string, resultChan chan result, revisionChan chan string, wg *sync.WaitGroup) {
	defer log.Print("Shutting down worker")
	defer wg.Done()
	log.Print("Starting worker")

	for revision := range revisionChan {
		result, err := processRevision(gitDir, tmpDir, revision)
		if err != nil {
			log.Printf("processing revision %s failed: %v", revision, err)
			continue
		}
		resultChan <- result
	}
}

func processRevision(gitDir, tmpDir string, revision string) (result, error) {
	workDir, err := mkWorkDir(gitDir, tmpDir)
	if err != nil {
		return result{}, err
	}
	defer os.RemoveAll(workDir)

	return getStats(workDir, revision)
}

var wcRegexp = regexp.MustCompile(`^\s+(\d+) total$`)

func getStats(workDir string, revision string) (result, error) {
	var buf bytes.Buffer
	cmd := exec.Command("git", "checkout", revision)
	cmd.Stderr = &buf
	cmd.Dir = workDir
	err := cmd.Run()
	if err != nil {
		return result{}, fmt.Errorf("git checkout failed: %v, output: %s", err, buf.String())
	}

	buf.Reset()
	cmd = exec.Command("git", "log", "-1", "--date=short", "--pretty=format:%cd")
	cmd.Stdout = &buf
	cmd.Stderr = os.Stderr
	cmd.Dir = workDir
	err = cmd.Run()
	if err != nil {
		return result{}, fmt.Errorf("git log failed: %v", err)
	}
	date := buf.String()

	buf.Reset()
	cmd = exec.Command("find", ".", `-type`, `f`, `(`, `-name`, `*.html`, `-o`, `-name`, `*.xml`, `-o`, `-name`, `*.java`, `-o`, `-name`, `*.py`, `)`)
	cmd.Dir = workDir
	cmd.Stdout = &buf
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		return result{}, fmt.Errorf("find failed: %v", err)
	}

	files := strings.Split(buf.String(), "\n")

	args := []string{"-l"}
	for _, file := range files {
		if len(file) > 0 {
			args = append(args, file)
		}
	}

	if len(args) == 1 {
		return result{date: date}, nil
	}

	buf.Reset()
	cmd = exec.Command("wc", args...)
	cmd.Dir = workDir
	cmd.Stdout = &buf
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		return result{}, fmt.Errorf("wc failed: %v", err)
	}

	lineCounts := strings.Split(buf.String(), "\n")
	totalLines := 0
	for _, lineCount := range lineCounts {
		matches := wcRegexp.FindStringSubmatch(lineCount)
		if len(matches) != 2 {
			continue
		}

		linesStr := matches[1]
		lines, err := strconv.Atoi(linesStr)
		if err != nil {
			return result{}, fmt.Errorf("unable to parse line count: %s", linesStr)
		}
		totalLines += lines
	}

	res := result{
		date:     date,
		lines:    totalLines,
	}

	return res, nil
}

func mkWorkDir(gitDir, tmpDir string) (string, error) {
	dir, err := ioutil.TempDir(tmpDir, "git_")
	if err != nil {
		return "", fmt.Errorf("unable to create temp dir: %v", err)
	}

	cmd := exec.Command("rsync", "-a", "--exclude=objects", gitDir+"/", dir+"/.git/")
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		return "", fmt.Errorf("rsync failed: %v", err)
	}

	cmd = exec.Command("ln", "-s", path.Join(gitDir, "objects"), path.Join(dir, ".git", "objects"))
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		return "", fmt.Errorf("ln failed: %v", err)
	}

	return dir, nil
}
