## About
The code is based on https://github.com/shraddhaag/1brc.
I have made some optimizations to improve its performance
through e.g., preallocation, sync.Pool, xx3hash, SwissTable, pointer, and so on.
Now the modified code takes about 6s to process 1 billion rows on my laptop (Apple M1 2021, 32GB)

## Prepare 1 Billion Rows
```sh
python calc.py 1000000000
```

## Run
```sh
go run main.go -input data/measurements.txt
```

## Profile
```sh
time go run main.go -input data/measurements.txt -cpuprofile cpuprofile -memprofile memprofile -execprofile execprofile
```
