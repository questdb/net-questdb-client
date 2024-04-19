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

// ReSharper disable UnusedAutoPropertyAccessor.Global
// ReSharper disable InconsistentNaming

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
namespace QuestDB.Utils;

// ReSharper disable once ClassNeverInstantiated.Global
/// <summary>
///     Represents the JSON error information returned when an ILP/HTTP send fails.
/// </summary>
public record JsonErrorResponse
{
    /// <summary>
    ///     An error code.
    /// </summary>
    public string code { get; init; }

    /// <summary>
    ///     An error message.
    /// </summary>
    public string message { get; init; }

    /// <summary>
    ///     The ILP line number related to the error.
    /// </summary>
    public int line { get; init; }

    /// <summary>
    ///     The error id.
    /// </summary>
    public string errorId { get; init; }

    public override string ToString()
    {
        return
            $"\nServer Response (\n\tCode: `{code}`\n\tMessage: `{message}`\n\tLine: `{line}`\n\tErrorId: `{errorId}` \n)";
    }
}