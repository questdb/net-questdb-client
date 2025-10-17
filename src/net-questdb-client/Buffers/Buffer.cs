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

namespace QuestDB.Buffers;

/// <summary>
///     A factory to create new buffers based the supported protocol version.
/// </summary>
public static class Buffer
{
    /// <summary>
    ///     Creates an IBuffer instance, based on the provided protocol version.
    /// </summary>
    /// <param name="bufferSize"></param>
    /// <param name="maxNameLen"></param>
    /// <param name="maxBufSize"></param>
    /// <param name="version"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    public static IBuffer Create(int bufferSize, int maxNameLen, int maxBufSize, ProtocolVersion version)
    {
        return version switch
        {
            ProtocolVersion.V1 => new BufferV1(bufferSize, maxNameLen, maxBufSize),
            ProtocolVersion.V2 => new BufferV2(bufferSize, maxNameLen, maxBufSize),
            ProtocolVersion.V3 => new BufferV3(bufferSize, maxNameLen, maxBufSize),
            ProtocolVersion.Auto => new BufferV3(bufferSize, maxNameLen, maxBufSize),
            _ => throw new NotImplementedException(),
        };
    }
}
