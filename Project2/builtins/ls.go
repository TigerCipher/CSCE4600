package builtins

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
)

func ListFiles(args ...string) error {
	// Parse command line flags
	fs := flag.NewFlagSet("ls", flag.ContinueOnError)
	longFormat := fs.Bool("l", false, "Use long listing format")
	showHidden := fs.Bool("a", false, "Show hidden files")
	recursive := fs.Bool("R", false, "Recursively list files and directories")
	sortByTime := fs.Bool("t", false, "Sort files by modification time")
	sortBySize := fs.Bool("S", false, "Sort files by size")
	reverseOrder := fs.Bool("r", false, "Reverse the order of the listing")
	humanReadable := fs.Bool("h", false, "Print file sizes in human-readable format")
	listDirectories := fs.Bool("d", false, "List directories themselves")

	// Parse the provided flags
	err := fs.Parse(args)
	if err != nil {
		return err
	}

	dirPath := "."
	if len(fs.Args()) > 0 {
		dirPath = fs.Args()[0]
	}

	files, err := os.ReadDir(dirPath)
	if err != nil {
		return err
	}

	// Apply sorting if required
	if *sortByTime {
		sort.Slice(files, func(i, j int) bool {
			fileInfoI, err := files[i].Info()
			if err != nil {
				return false
			}
			fileInfoJ, err := files[j].Info()
			if err != nil {
				return false
			}
			return fileInfoI.ModTime().After(fileInfoJ.ModTime())
		})
	} else if *sortBySize {
		sort.Slice(files, func(i, j int) bool {
			fileInfoI, err := files[i].Info()
			if err != nil {
				return false
			}
			fileInfoJ, err := files[j].Info()
			if err != nil {
				return false
			}
			return fileInfoI.Size() > fileInfoJ.Size()
		})
	}

	// Reverse the order if required
	if *reverseOrder {
		for i := len(files)/2 - 1; i >= 0; i-- {
			opp := len(files) - 1 - i
			files[i], files[opp] = files[opp], files[i]
		}
	}

	for _, file := range files {
		if !*showHidden && file.Name()[0] == '.' {
			continue
		}

		if *longFormat {
			// Perform long listing format
			fileInfo, err := file.Info()
			if err != nil {
				return err
			}
			printFileInfo(fileInfo, file.Name(), *humanReadable)
		} else {
			// Perform regular listing format
			fmt.Println(file.Name())
		}

		// Recursively list directories if required
		if *recursive && file.IsDir() && !*listDirectories {
			err := ListFiles(dirPath + "/" + file.Name())
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func printFileInfo(fileInfo os.FileInfo, name string, humanReadable bool) {
	mode := fileInfo.Mode()
	size := fileInfo.Size()
	modTime := fileInfo.ModTime()

	// Format the file size
	var sizeStr string
	if size == 0 {
		sizeStr = "0"
	} else if humanReadable {
		sizeStr = formatSize(size)
	} else {
		sizeStr = strconv.FormatInt(size, 10)
	}

	// Print the file information
	fmt.Printf("%s %s %s  ", mode, sizeStr, modTime.Format("Jan _2 15:04"))

	// Print the file name
	fmt.Println(name)
}

func formatSize(size int64) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}
	div, exp := int64(unit), 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(size)/float64(div), "KMGTPE"[exp])
}
