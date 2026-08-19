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
            QwpStatusCode.NotWritable => SenderErrorCategory.NotWritable,
            QwpStatusCode.DictionaryGap => SenderErrorCategory.DictionaryGap,
            _ => SenderErrorCategory.Unknown,
        };

    // NACK policy v2: never drop. WRITE_ERROR / INTERNAL_ERROR retry (a transient server-side
    // condition), UNKNOWN retries fail-open (a future status byte must not kill old clients), and
    // only rejections that are deterministic under byte-identical replay latch terminal.
    public static SenderErrorPolicy DefaultPolicy(SenderErrorCategory category) =>
        category switch
        {
            SenderErrorCategory.WriteError => SenderErrorPolicy.Retriable,
            SenderErrorCategory.InternalError => SenderErrorPolicy.Retriable,
            SenderErrorCategory.NotWritable => SenderErrorPolicy.Retriable,
            SenderErrorCategory.DictionaryGap => SenderErrorPolicy.Retriable,
            SenderErrorCategory.Unknown => SenderErrorPolicy.Retriable,
            _ => SenderErrorPolicy.Terminal,
        };

    // Categories whose default is Terminal are deterministic rejections — a user resolver may not
    // downgrade them to Retriable (that would spin the reconnect machinery on a poison frame
    // forever). Retriable-default categories may be biased up to Terminal (e.g. on_write_error=halt).
    public static SenderErrorPolicy ResolvePolicy(
        SenderErrorCategory category,
        SenderErrorPolicyResolver? resolver)
    {
        var fallback = DefaultPolicy(category);
        if (fallback == SenderErrorPolicy.Terminal)
        {
            return SenderErrorPolicy.Terminal;
        }
        return resolver?.Invoke(category) ?? fallback;
    }
}
