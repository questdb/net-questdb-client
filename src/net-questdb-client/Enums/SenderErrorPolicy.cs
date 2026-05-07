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

namespace QuestDB.Enums;

/// <summary>
///     Policy applied by the client when a <see cref="SenderErrorCategory" /> fires.
///     <see cref="SenderErrorCategory.ProtocolViolation" /> and
///     <see cref="SenderErrorCategory.Unknown" /> are forced <see cref="Halt" />.
/// </summary>
public enum SenderErrorPolicy
{
    /// <summary>
    ///     Drop the rejected batch from the SF disk store (advance the ack watermark past it)
    ///     and continue draining subsequent batches. The data is lost from the sender's perspective.
    /// </summary>
    DropAndContinue,

    /// <summary>
    ///     Latch the error as terminal. The next producer-thread API call throws
    ///     <see cref="Utils.LineSenderServerException" />. The sender does not drain further
    ///     until the caller closes and rebuilds it.
    /// </summary>
    Halt,
}
