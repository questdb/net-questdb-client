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
///     Callback that populates the typed bind parameters for a single
///     <c>QwpQueryClient.Execute</c> invocation. The .NET counterpart of Java's
///     <c>QwpBindSetter</c> on java-questdb-client main 64b7ee69.
/// </summary>
/// <remarks>
///     Implementations must call the typed setters on the supplied
///     <see cref="QwpBindValues"/> in strictly ascending index order starting at 0,
///     with no gaps. <see cref="QwpBindValues"/> validates the order and throws on
///     violation.
/// </remarks>
internal delegate void QwpBindSetter(QwpBindValues binds);
