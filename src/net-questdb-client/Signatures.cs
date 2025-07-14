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

using System.Reflection;

namespace QuestDB;

/// <summary>
/// </summary>
public static class Signatures
{
    private static readonly Lazy<ISignatureGenerator> SigGen = new(() => CreateSignatureGenerator0());

    private static ISignatureGenerator CreateSignatureGenerator0()
    {
        Exception? ex = null;
        try
        {
            var assembly = Assembly.LoadFrom("net-questdb-client-tcp-auth.dll");
            var type     = assembly.GetType("QuestDB.Secp256r1SignatureGenerator");
            if (type != null)
            {
                var val = (ISignatureGenerator?)Activator.CreateInstance(type);
                if (val != null)
                {
                    return val;
                }
            }
        }
        catch (Exception e)
        {
            ex = e;
        }

        throw new TypeLoadException(
            "Could not load QuestDB.Secp256r1SignatureGenerator, please add a reference to assembly \"net-questdb-client-tcp-auth\"" +
            (ex == null ? ": cannot load the type, return value is null" : ""), ex);
    }

    /// <summary>
    /// </summary>
    public static ISignatureGenerator CreateSignatureGenerator()
    {
        return SigGen.Value;
    }
}