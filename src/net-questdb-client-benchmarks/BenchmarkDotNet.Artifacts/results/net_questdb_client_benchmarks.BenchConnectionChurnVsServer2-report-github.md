```

BenchmarkDotNet v0.13.12, macOS Sonoma 14.6.1 (23G93) [Darwin 23.6.0]
Apple M1, 1 CPU, 8 logical and 8 physical cores
.NET SDK 9.0.101
  [Host] : .NET 9.0.0 (9.0.24.52809), Arm64 RyuJIT AdvSIMD

Job=MediumRun  Toolchain=InProcessNoEmitToolchain  IterationCount=15  
LaunchCount=1  WarmupCount=10  

```
| Method                  | BatchSize | ConnectionLimit | NumberOfTables | RowsPerIteration | Mean     | Error     | StdDev    |
|------------------------ |---------- |---------------- |--------------- |----------------- |---------:|----------:|----------:|
| **HttpRandomTableEveryRow** | **5000**      | **1**               | **10**             | **100000**           | **44.68 ms** |  **3.335 ms** |  **2.604 ms** |
| TcpRandomTableEveryRow  | 5000      | 1               | 10             | 100000           | 32.14 ms |  9.528 ms |  8.913 ms |
| **HttpRandomTableEveryRow** | **5000**      | **4**               | **10**             | **100000**           | **43.29 ms** |  **3.398 ms** |  **3.178 ms** |
| TcpRandomTableEveryRow  | 5000      | 4               | 10             | 100000           | 46.26 ms | 19.924 ms | 18.637 ms |
| **HttpRandomTableEveryRow** | **5000**      | **16**              | **10**             | **100000**           | **46.13 ms** |  **2.873 ms** |  **2.688 ms** |
| TcpRandomTableEveryRow  | 5000      | 16              | 10             | 100000           | 29.94 ms |  7.944 ms |  7.042 ms |
