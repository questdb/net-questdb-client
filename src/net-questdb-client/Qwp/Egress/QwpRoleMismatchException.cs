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

namespace QuestDB.Qwp.Egress;

/// <summary>
///     Thrown by <c>QwpQueryClient.Connect</c> (PR 11) when every configured endpoint
///     reports a role that does not satisfy the client's <c>target=</c> filter.
///     The .NET counterpart of Java's <c>QwpRoleMismatchException</c> on
///     java-questdb-client main 64b7ee69.
/// </summary>
/// <remarks>
///     Callers can distinguish this from a generic transport failure to tailor retry
///     behaviour: "no primary available" is worth waiting and retrying after a
///     failover window; "all endpoints down" is a harder failure.
/// </remarks>
internal sealed class QwpRoleMismatchException : Exception
{
    public QwpRoleMismatchException(string targetRole, QwpServerInfo? lastObserved, string message)
        : base(message)
    {
        TargetRole = targetRole;
        LastObserved = lastObserved;
    }

    /// <summary>
    ///     <see cref="QwpServerInfo"/> from the last endpoint the client tried before
    ///     giving up, or null if no endpoint responded with a SERVER_INFO frame.
    /// </summary>
    public QwpServerInfo? LastObserved { get; }

    /// <summary>The role filter the caller asked for (e.g. <c>"PRIMARY"</c>).</summary>
    public string TargetRole { get; }
}
