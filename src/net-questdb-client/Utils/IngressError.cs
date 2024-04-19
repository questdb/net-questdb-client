// ReSharper disable CommentTypo
/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

namespace QuestDB.Utils;

/// <summary>
///     Custom exception for ILP-related errors.
/// </summary>
public class IngressError : Exception
{
    /// <summary>
    ///     Constructs <see cref="IngressError" /> from a code and message.
    /// </summary>
    /// <param name="code">The error code</param>
    /// <param name="message">The error message</param>
    public IngressError(ErrorCode code, string? message)
        : base($"{code.ToString()} : {message}")
    {
        this.code = code;
    }

    /// <summary>
    ///     Constructs <see cref="IngressError" /> from a code, message and inner exception.
    /// </summary>
    /// <param name="code">The error ode</param>
    /// <param name="message">The error message</param>
    /// <param name="inner">The inner exception</param>
    public IngressError(ErrorCode code, string? message, Exception inner)
        : base($"{code.ToString()} : {message}", inner)
    {
        this.code = code;
    }

    // ReSharper disable once UnusedAutoPropertyAccessor.Global
    // ReSharper disable once InconsistentNaming
    /// <summary>
    ///     The custom error code.
    /// </summary>
    public ErrorCode code { get; }
}