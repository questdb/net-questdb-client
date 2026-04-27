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
 ******************************************************************************/

namespace QuestDB.Qwp;

/// <summary>
///     Thrown when an inbound QWP frame is malformed or references unknown connection state.
///     Mirrors Java's <c>QwpDecodeException</c> on java-questdb-client main 64b7ee69.
/// </summary>
internal sealed class QwpDecodeException : Exception
{
    public QwpDecodeException(string message) : base(message) { }
    public QwpDecodeException(string message, Exception inner) : base(message, inner) { }
}
