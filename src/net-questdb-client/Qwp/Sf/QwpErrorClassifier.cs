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

namespace QuestDB.Qwp.Sf;

internal static class QwpErrorClassifier
{
    public static SenderErrorCategory Classify(QwpStatusCode status) =>
        status switch
        {
            QwpStatusCode.SchemaMismatch => SenderErrorCategory.SchemaMismatch,
            QwpStatusCode.ParseError => SenderErrorCategory.ParseError,
            QwpStatusCode.InternalError => SenderErrorCategory.InternalError,
            QwpStatusCode.SecurityError => SenderErrorCategory.SecurityError,
            QwpStatusCode.WriteError => SenderErrorCategory.WriteError,
            _ => SenderErrorCategory.Unknown,
        };

    public static SenderErrorPolicy DefaultPolicy(SenderErrorCategory category) =>
        category switch
        {
            SenderErrorCategory.SchemaMismatch => SenderErrorPolicy.DropAndContinue,
            SenderErrorCategory.WriteError => SenderErrorPolicy.DropAndContinue,
            _ => SenderErrorPolicy.Halt,
        };

    public static SenderErrorPolicy ResolvePolicy(
        SenderErrorCategory category,
        SenderErrorPolicyResolver? resolver)
    {
        if (category is SenderErrorCategory.ProtocolViolation or SenderErrorCategory.Unknown)
        {
            return SenderErrorPolicy.Halt;
        }
        return resolver?.Invoke(category) ?? DefaultPolicy(category);
    }
}
