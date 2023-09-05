package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	commcid "github.com/filecoin-project/go-fil-commcid"
	"github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/tech-greedy/generate-car/util"
	"github.com/urfave/cli/v2"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"sync"
)

type CommpResult struct {
	commp     string
	pieceSize uint64
}

type Result struct {
	Ipld      *util.FsNode
	DataCid   string
	PieceCid  string
	PieceSize uint64
	CidMap    map[string]util.CidMapValue
}

type SimpleResult struct {
	DataCid   string
	PieceCid  string
	PieceSize uint64
	CarSize   int64
	FileName  string
}

type Input []util.Finfo

type CarHeader struct {
	Roots   []cid.Cid
	Version uint64
}

func init() {
	cbor.RegisterCborType(CarHeader{})
}

const BufSize = (4 << 20) / 128 * 127

func main() {
	ctx := context.TODO()
	app := &cli.App{
		Name:  "generate-car",
		Usage: "generate car archive from list of files and compute commp in the mean time",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "single",
				Usage: "When enabled, it indicates that the input is a single file or folder to be included in full, instead of a spec JSON",
			},
			&cli.BoolFlag{
				Name:  "simple",
				Usage: "simple output,only output deal required parameters",
				Value: true,
			},
			&cli.StringFlag{
				Name:    "input",
				Aliases: []string{"i"},
				Usage:   "When --single is specified, this is the file or folder to be included in full. Otherwise this is a JSON file containing the list of files to be included in the car archive",
				Value:   "-",
			},
			&cli.StringFlag{
				Name:  "input-json",
				Usage: "value Input each line is a json",
				Value: "",
			},
			&cli.Uint64Flag{
				Name:    "piece-size",
				Aliases: []string{"s"},
				Usage:   "Target piece size, default to minimum possible value",
				Value:   0,
			},
			&cli.StringFlag{
				Name:    "out-dir",
				Aliases: []string{"o"},
				Usage:   "Output directory to save the car file",
				Value:   ".",
			},
			&cli.StringFlag{
				Name:    "tmp-dir",
				Aliases: []string{"t"},
				Usage:   "Optionally copy the files to a temporary (and much faster) directory",
				Value:   "",
			},
			&cli.StringFlag{
				Name:     "parent",
				Aliases:  []string{"p"},
				Usage:    "Parent path of the dataset",
				Required: true,
			},
			&cli.IntFlag{
				Name:  "parallel",
				Usage: "value Number of parallels",
				Value: 1,
			},
		},
		Action: func(c *cli.Context) error {
			inputFile := c.String("input")
			inputJsonFile := c.String("input-json")
			pieceSizeInput := c.Uint64("piece-size")
			outDir := c.String("out-dir")
			parent := c.String("parent")
			tmpDir := c.String("tmp-dir")
			single := c.Bool("single")
			simple := c.Bool("simple")
			parallel := c.Int("parallel")
			qiniuConfPath := os.Getenv("QINIU")

			inputList, err := getInputList(inputFile, single, inputJsonFile)
			if err != nil {
				return err
			}

			inputChan := make(chan Input)
			waiter := &sync.WaitGroup{}
			for i := 0; i < parallel; i++ {
				waiter.Add(1)
				go func() {
					for {
						select {
						case input, ok := <-inputChan:
							if !ok {
								waiter.Done()
								return
							}
							if err := generateCar(ctx, input, parent, tmpDir, outDir, pieceSizeInput, qiniuConfPath, simple); err != nil {
								log.Fatal(err)
							}
						case <-ctx.Done():
							waiter.Done()
							return
						}
					}
				}()
			}

			for _, input := range inputList {
				inputChan <- input
			}
			close(inputChan)
			waiter.Wait()

			return nil
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func getInputList(inputFile string, single bool, inputJsonFile string) ([]Input, error) {
	var inputList []Input

	if inputJsonFile != "" {
		f, err := os.Open(inputJsonFile)
		if err != nil {
			return nil, err
		}
		defer f.Close()

		r := bufio.NewReader(f)
		for {
			lineBytes, err := r.ReadBytes('\n')
			if err != nil && err != io.EOF {
				return nil, err
			}

			// 防止文件末尾没有'\n'换行导致的 EOF 错误
			if err == io.EOF && len(lineBytes) == 0 {
				break
			}

			var input Input
			err = json.Unmarshal(lineBytes, &input)
			if err != nil {
				return nil, err
			}

			inputList = append(inputList, input)
		}
		return inputList, nil
	}

	// 原来的方式导入 input
	var input Input
	if single {
		stat, err := os.Stat(inputFile)
		if err != nil {
			return nil, err
		}
		if stat.IsDir() {
			err := filepath.Walk(inputFile, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if info.IsDir() {
					return nil
				}
				input = append(input, util.Finfo{
					Path:  path,
					Size:  info.Size(),
					Start: 0,
					End:   info.Size(),
				})
				return nil
			})
			if err != nil {
				return nil, err
			}
		} else {
			input = append(input, util.Finfo{
				Path:  inputFile,
				Size:  stat.Size(),
				Start: 0,
				End:   stat.Size(),
			})
		}
	} else {
		var inputBytes []byte
		if inputFile == "-" {
			reader := bufio.NewReader(os.Stdin)
			buf := new(bytes.Buffer)
			_, err := buf.ReadFrom(reader)
			if err != nil {
				return nil, err
			}
			inputBytes = buf.Bytes()
		} else {
			inputFileBytes, err := os.ReadFile(inputFile)
			if err != nil {
				return nil, err
			}
			inputBytes = inputFileBytes
		}
		err := json.Unmarshal(inputBytes, &input)
		if err != nil {
			return nil, err
		}
	}

	return append(inputList, input), nil
}

func generateCar(ctx context.Context, input Input, parent string, tmpDir string, outDir string, pieceSizeInput uint64, qiniuConfPath string, simple bool) error {
	outFilename := uuid.New().String() + ".car"
	outPath := path.Join(outDir, outFilename)
	carF, err := os.Create(outPath)
	if err != nil {
		return err
	}
	cp := new(commp.Calc)
	writer := bufio.NewWriterSize(io.MultiWriter(carF, cp), BufSize)
	ipld, cid, cidMap, err := util.GenerateCar(ctx, input, parent, tmpDir, writer)
	if err != nil {
		return err
	}
	err = writer.Flush()
	if err != nil {
		return err
	}

	stat, err := carF.Stat()
	if err != nil {
		return err
	}
	carFileSize := stat.Size()

	err = carF.Close()
	if err != nil {
		return err
	}
	rawCommP, pieceSize, err := cp.Digest()
	if err != nil {
		return err
	}
	if pieceSizeInput > 0 {
		rawCommP, err = commp.PadCommP(
			rawCommP,
			pieceSize,
			pieceSizeInput,
		)
		if err != nil {
			return err
		}
		pieceSize = pieceSizeInput
	}
	commCid, err := commcid.DataCommitmentV1ToCID(rawCommP)
	if err != nil {
		return err
	}

	fileName := commCid.String() + ".car"

	// TODO: 更新为：上传到七牛
	if qiniuConfPath != "" {
		err = util.SubmitPath(qiniuConfPath, path.Join(outDir, fileName), outPath)
	} else {
		err = os.Rename(outPath, path.Join(outDir, fileName))
	}
	if err != nil {
		return err
	}

	var output []byte
	if simple {
		output, err = json.Marshal(SimpleResult{
			DataCid:   cid,
			PieceCid:  commCid.String(),
			PieceSize: pieceSize,
			CarSize:   carFileSize,
			FileName:  fileName,
		})
	} else {
		output, err = json.Marshal(Result{
			Ipld:      ipld,
			DataCid:   cid,
			PieceCid:  commCid.String(),
			PieceSize: pieceSize,
			CidMap:    cidMap,
		})
	}

	if err != nil {
		return err
	}
	fmt.Println(string(output))
	return nil
}
