package main

import (
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
	"sort"
	"runtime/pprof"
	"os/signal"
	"syscall"
	"path/filepath"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
	"io"
)

type commit struct {
	date  string
	hash  string
	lines int
}

func main() {
	app := kingpin.New("git-sloc", "Get historic source line counts out of a git repository")
	gitDir := app.Arg("SRC", "git directory (usually called '.git')").Default(".git").ExistingDir()
	numWorkers := app.Flag("num-workers", "number of worker goroutines").Default("1").Uint8()
	tmpDir := app.Flag("tmp", "directory for temporary files").Default(os.TempDir()).String()
	suffixes := app.Flag("suffixes", "file name suffixes to take into account, eg. '.java'. at least one required").Short('s').Strings()
	cpuProfile := app.Flag("cpu-profile", "write CPU profile to file").String()
	_, err := app.Parse(os.Args[1:])
	if err != nil {
		log.Fatalf("parsing command line failed: %v", err)
	}

	if len(*suffixes) < 1 {
		log.Fatalf("at least one suffix required, see --help")
	}

	if len(*cpuProfile) > 0 {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	commits, err := revisions(*gitDir)
	if err != nil {
		log.Fatal(err)
	}

	workerChan := make(chan commit)
	aggregatorChan := make(chan commit)

	log.Printf("Starting workers...")
	var workerWg sync.WaitGroup
	for i := 0; i < int(*numWorkers); i++ {
		workerWg.Add(1)
		go worker(*gitDir, *tmpDir, aggregatorChan, workerChan, &workerWg, *suffixes)
	}

	var wgAggregator sync.WaitGroup
	wgAggregator.Add(1)
	go aggregator(len(commits), aggregatorChan, &wgAggregator)

	// handle signals
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT)

	// start producer
	go producer(sigChan, workerChan, commits)

	workerWg.Wait()
	log.Printf("All workers done. Closing aggregatorChan.")
	close(aggregatorChan)

	log.Printf("Waiting for aggregator")
	wgAggregator.Wait()
}

func producer(sigChan <- chan os.Signal, workerChan chan <- commit, commits []commit) {
	defer close(workerChan)
	defer log.Printf("Shutting down producer")

	var i int
	for {
		select {
		case sig := <-sigChan:
			if sig == syscall.SIGINT {
				log.Printf("Interrupted, shutting down...")
				return
			}
		case workerChan <- commits[i]:
			i++
			if i == len(commits) {
				return
			}
		}
	}
}

func aggregator(numCommits int, resultChan <-chan commit, wg *sync.WaitGroup) {
	defer wg.Done()
	defer log.Printf("Shutting down aggregator")

	log.Printf("Starting aggregator")

	start := time.Now()
	histogram := make(map[string]int)
	processedResults := 0

	for result := range resultChan {
		processedResults++
		if processedResults%100 == 0 {
			shareDone := float64(processedResults) / float64(numCommits)

			projectedTotalDuration := time.Duration(float64(time.Since(start).Seconds()) / shareDone) * time.Second
			eta := projectedTotalDuration - time.Since(start).Round(time.Second)
			log.Printf("%.2f%% done, eta %v", shareDone*100, eta)
		}

		if result.lines == 0 {
			continue
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
		fmt.Printf("%s %d\n", date, histogram[date])
	}

	log.Printf("Total time: %v", time.Since(start))
}

func revisions(gitDir string) ([]commit, error) {
	var buf bytes.Buffer
	cmd := exec.Command("git", "--git-dir="+gitDir, "log", "--pretty=format:%H %cd", "--date=short")
	cmd.Stdout = &buf
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("git log failed: %v", err)
	}

	var revisions []commit
	for _, line := range strings.Split(buf.String(), "\n") {
		tokens := strings.Split(line, " ")
		if len(tokens) != 2 {
			return nil, fmt.Errorf("invalid git log output: %s", line)
		}

		revisions = append(revisions, commit{hash: tokens[0], date: tokens[1]})
	}

	return revisions, nil
}

func worker(gitDir, tmpDir string, aggregatorChan chan commit, workerChan chan commit, wg *sync.WaitGroup, suffixes []string) {
	defer log.Print("Shutting down worker")
	defer wg.Done()
	log.Print("Starting worker")

	workDir, err := mkWorkDir(gitDir, tmpDir)
	if err != nil {
		log.Fatalf("failed to create work dir: %v", err)
	}
	defer os.RemoveAll(workDir)

	log.Printf("workDir=%s", workDir)

	for commit := range workerChan {
		result, err := getLinesForCommit(workDir, commit, suffixes)
		if err != nil {
			log.Printf("processing revision %s failed: %v", commit, err)
			continue
		}
		aggregatorChan <- result
	}
}

func getLinesForCommit(workDir string, c commit, suffixes []string) (commit, error) {
	var buf bytes.Buffer
	cmd := exec.Command("git", "checkout", "-f", c.hash)
	cmd.Stderr = &buf
	cmd.Dir = workDir
	err := cmd.Run()
	if err != nil {
		return commit{}, fmt.Errorf("git checkout failed: %v, output: %s", err, buf.String())
	}

	files, err := findFiles(workDir, suffixes)
	if err != nil {
		return commit{}, fmt.Errorf("failed to find files: %v", err)
	}

	lines, err := countLines(files)
	if err != nil {
		return commit{}, fmt.Errorf("failed to count lines: %v", err)
	}
	c.lines = lines

	return c, nil
}

func countLines(files []string) (int, error) {
	var lines int
	for _, file := range files {
		n, err := countLinesForFile(file)
		if err != nil {
			return 0, err
		}
		lines += n
	}
	return lines, nil
}

func countLinesForFile(file string) (int, error) {
	f, err := os.Open(file)
	if err != nil {
		return 0, fmt.Errorf("unable to open file: %s: %v", file, err)
	}
	defer f.Close()

	buf := make([]byte, 1024)
	var lines int
	for {
		n, err := f.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("unable to read file: %s: %v", file, err)
		}

		for i := 0; i < n; i++ {
			if buf[i] == '\n' {
				lines++
			}
		}
	}
	return lines, nil
}

func findFiles(dir string, suffixes []string) (files []string, err error) {
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		name := info.Name()
		for _, suffix := range suffixes {
			if strings.HasSuffix(name, suffix){
				files = append(files, path)
				return nil
			}
		}
		return nil
	})
	return
}

func mkWorkDir(gitDir, tmpDir string) (string, error) {
	dir, err := ioutil.TempDir(tmpDir, "git_")
	if err != nil {
		return "", fmt.Errorf("unable to create temp dir: %v", err)
	}

	// do not rsync the objects folder as it is very large
	cmd := exec.Command("rsync", "-a", "--exclude=objects", gitDir+"/", dir+"/.git/")
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		return "", fmt.Errorf("rsync failed: %v", err)
	}

	// link the objects folder instead
	cmd = exec.Command("ln", "-s", path.Join(gitDir, "objects"), path.Join(dir, ".git", "objects"))
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		return "", fmt.Errorf("ln failed: %v", err)
	}

	return dir, nil
}
