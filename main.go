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

type revision struct {
	date     string
	revision string
}

type result struct {
	date     string
	revision string
	lines    int
}

func main() {
	numWorkers := flag.Int("num-workers", 1, "number of worker goroutines")
	tmpDir := flag.String("tmp", os.TempDir(), "directory for temporary files")
	// revisionsFile := flag.String("revisions-cache", "", "file used to read/write information per revision to speed up computation")
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	flag.Parse()
	if flag.NArg() != 1 {
		log.Fatalf("Usage: %s <path to git dir>", os.Args[0])
	}
	gitDir := flag.Arg(0)

	if len(*cpuprofile) > 0 {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	revisions, err := revisions(gitDir)
	if err != nil {
		log.Fatal(err)
	}

	numRevisions := len(revisions)

	revisionChan := make(chan revision)
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

			projectedTotalDuration := time.Duration(float64(time.Since(start).Seconds()) / shareDone) * time.Second
			eta := projectedTotalDuration - time.Since(start).Round(time.Second)
			log.Printf("%.2f%% done, eta %v", shareDone*100, eta)
		}

		// log.Printf("%s: %d", result.revision, result.lines)

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

func revisions(gitDir string) ([]revision, error) {
	var buf bytes.Buffer
	cmd := exec.Command("git", "--git-dir="+gitDir, "log", "--pretty=format:%H %cd", "--date=short")
	cmd.Stdout = &buf
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("git log failed: %v", err)
	}

	var revisions []revision
	for _, line := range strings.Split(buf.String(), "\n") {
		tokens := strings.Split(line, " ")
		if len(tokens) != 2 {
			return nil, fmt.Errorf("invalid git log output: %s", line)
		}

		revisions = append(revisions, revision{revision: tokens[0], date: tokens[1]})
	}

	return revisions, nil
}

func worker(gitDir, tmpDir string, resultChan chan result, revisionChan chan revision, wg *sync.WaitGroup) {
	defer log.Print("Shutting down worker")
	defer wg.Done()
	log.Print("Starting worker")

	workDir, err := mkWorkDir(gitDir, tmpDir)
	if err != nil {
		log.Fatalf("failed to create work dir: %v", err)
	}
	// defer os.RemoveAll(workDir)

	log.Printf("workDir=%s", workDir)

	for revision := range revisionChan {
		result, err := getStats(workDir, revision)
		if err != nil {
			log.Printf("processing revision %s failed: %v", revision, err)
			continue
		}
		resultChan <- result
	}
}

var wcRegexp = regexp.MustCompile(`^\s+(\d+) total$`)

func getStats(workDir string, revision revision) (result, error) {
	//filesToDelete, err := filepath.Glob(path.Join(workDir, "*"))
	//if err != nil {
	//	return result{}, fmt.Errorf("globbing failed: %v", err)
	//}
	//
	//log.Printf("filesToDelete: %v", filesToDelete)
	//
	//for _, toDelete := range filesToDelete {
	//	// do not delete .git directory ;)
	//	if strings.Contains(toDelete, ".git") {
	//		continue
	//	}
	//
	//	err := os.RemoveAll(toDelete)
	//	if err != nil {
	//		return result{}, fmt.Errorf("removing %s failed: %v", toDelete, err)
	//	}
	//}
	//
	//filesInWorkdir, err := filepath.Glob(path.Join(workDir, "*"))
	//if err != nil {
	//	return result{}, fmt.Errorf("globbing failed: %v", err)
	//}
	//log.Printf("filesInWorkdir: %v", filesInWorkdir)

	var buf bytes.Buffer
	log.Printf("git checkout %s", revision.revision)
	cmd := exec.Command("git", "checkout", "-f", revision.revision/*, "--", "."*/)
	cmd.Stderr = &buf
	cmd.Dir = workDir
	err := cmd.Run()
	if err != nil {
		return result{}, fmt.Errorf("git checkout failed: %v, output: %s", err, buf.String())
	}

	var allFiles []string
	dirs := []string{workDir}

	for _, dir := range dirs {
		files, err := ioutil.ReadDir(dir)
		if err != nil {
			return result{}, err
		}

		for _, f := range files {
			if f.IsDir() {
				dirs = append(dirs, path.Join(dir, f.Name()))
			}
			name := f.Name()
			if strings.HasSuffix(name, ".java") || strings.HasSuffix(name, ".xml") || strings.HasSuffix(name, ".html") || strings.HasSuffix(name, ".py") {
				allFiles = append(allFiles, path.Join(dir, name))
			}
		}
	}




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

	log.Printf("walker: %d, find: %d", len(allFiles), len(files))

	args := []string{"-l"}
	for _, file := range files {
		if len(file) > 0 {
			args = append(args, file)
		}
	}

	if len(args) == 1 {
		return result{revision: revision.revision, date: revision.date}, nil
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
		date:     revision.date,
		revision: revision.revision,
		lines:    totalLines,
	}

	return res, nil
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
