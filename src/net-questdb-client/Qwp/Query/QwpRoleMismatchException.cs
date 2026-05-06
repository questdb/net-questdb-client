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

using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Qwp.Query;

/// <summary>
///     Thrown by <c>connect()</c> when no configured endpoint matches the requested
///     <c>target=any|primary|replica</c> filter. The <see cref="LastObserved" /> property carries
///     the last server's <c>SERVER_INFO</c> so callers can distinguish "no primary available" from
///     "all endpoints unreachable".
/// </summary>
public sealed class QwpRoleMismatchException : IngressError
{
    /// <summary>
    ///     Constructs the exception with the requested role filter, the most recent
    ///     <c>SERVER_INFO</c> observed (if any), and a human-readable message.
    /// </summary>
    public QwpRoleMismatchException(TargetType target, QwpServerInfo? lastObserved, string message)
        : base(ErrorCode.ConfigError, message)
    {
        Target = target;
        LastObserved = lastObserved;
    }

    /// <summary>The role filter the caller requested when establishing the connection.</summary>
    public TargetType Target { get; }

    /// <summary>The last server's <c>SERVER_INFO</c> if any was received, otherwise <c>null</c>.</summary>
    public QwpServerInfo? LastObserved { get; }
}
