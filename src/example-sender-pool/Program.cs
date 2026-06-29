/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

using System;
using System.Linq;
using System.Threading.Tasks;
using QuestDB;

// A single ISender is not thread-safe. To ingest from many threads, construct one shared
// QuestDBClient pool and borrow a sender per unit of work. Disposing the borrowed sender flushes
// its rows and returns it to the pool — the underlying connection stays open and is reused.

await using var client = QuestDBClient.Builder()
    .FromConfig("http::addr=localhost:9000;")
    .SenderPoolMin(2)
    .SenderPoolMax(8)
    .AcquireTimeout(TimeSpan.FromSeconds(5))
    .Build();

// Fan out work across threads; each borrows independently from the shared pool.
await Parallel.ForEachAsync(Enumerable.Range(0, 100), async (i, ct) =>
{
    using var sender = client.BorrowSender();
    sender.Table("trades")
          .Symbol("symbol", "ETH-USD")
          .Column("price", 2615.54 + i)
          .Column("amount", 0.00044);
    await sender.AtAsync(DateTime.UtcNow, ct);
    await sender.SendAsync(ct);
}); // each `using` returns its sender to the pool

Console.WriteLine($"Done. pool: {client.AvailableSenderCount} idle / {client.TotalSenderCount} total");

// Dedicated long-lived producer thread? Pin one sender to the thread instead of borrowing each time:
//   var s = client.Sender();   // first call pins; later calls on this thread return the same sender
//   ... s.Table(...).At(...); s.Send();
//   client.ReleaseSender();    // return the pinned sender when the thread is done
