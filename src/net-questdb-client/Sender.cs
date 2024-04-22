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
using QuestDB.Senders;
using QuestDB.Utils;

// ReSharper disable MemberCanBePrivate.Global

namespace QuestDB;

/// <summary>
///     A factory for creating new instances of <see cref="ISender"/>
/// </summary>
/// <remarks>
///     For sole initialisation via config string, please use <see cref="New(string)"/>. This does not require a call to <see cref="SenderOptions.Build"/>.
///     <para />
///     If you wish set initial options via config string, and then modify others, please use <see cref="Configure(string)"/>,
///     followed by record syntax, followed by <see cref="SenderOptions.Build"/>.
///     <para />
///     If you wish to configure entirely programmatically, please use <see cref="New(SenderOptions)"/>.
/// </remarks>
public static class Sender
{
    /// <summary>
    ///     Creates and initialises a new instance of <see cref="ISender"/> from a configuration string.
    /// </summary>
    /// <param name="confStr"></param>
    /// <returns>An initialised <see cref="ISender"/></returns>
    public static ISender New(string confStr)
    {
        return Configure(confStr).Build();
    }
    
    /// <summary>
    ///     Performs initial configuration of <see cref="ISender"/>.
    ///     Must be followed by <see cref="SenderOptions.Build"/> prior to use.
    /// </summary>
    /// <param name="options"></param>
    /// <returns></returns>
    /// <exception cref="NotSupportedException"></exception>
    public static ISender New(SenderOptions? options = null)
    {
        if (options is null)
        {
            return new HttpSender("http::addr=localhost:9000;");
        }
        
        switch (options.protocol)
        {
            case ProtocolType.http:
            case ProtocolType.https:
                return new HttpSender(options);
            case ProtocolType.tcp:
            case ProtocolType.tcps:
                return new TcpSender(options);
            default:
                throw new NotSupportedException();
        }
    }
    
    /// <summary>
    ///     Begins configuring a sender. Must be followed by <see cref="SenderOptions.Build"/>.
    /// </summary>
    /// <param name="confStr"></param>
    /// <returns></returns>
    public static SenderOptions Configure(string confStr)
    {
        return new SenderOptions(confStr);
    }
}