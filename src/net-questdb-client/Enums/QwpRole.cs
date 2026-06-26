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
///     Server role advertised in the QWP SERVER_INFO frame. Backed by <see cref="byte" /> to match the
///     single-byte wire encoding; an unrecognised wire value casts to an unnamed member rather than failing.
/// </summary>
public enum QwpRole : byte
{
    /// <summary>Single, non-replicated node.</summary>
    Standalone = 0x00,
    
    /// <summary>Primary node in a replicated cluster.</summary>
    Primary = 0x01,

    /// <summary>Read replica.</summary>
    Replica = 0x02,

    /// <summary>Former primary still catching up after a failover; transiently unavailable for writes.</summary>
    PrimaryCatchup = 0x03,
    
    /// <summary>
    /// Server value does not map to any known category
    /// </summary>
    Undefined = byte.MaxValue
}
