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

namespace QuestDB.Utils;

/// <summary>
/// Manages round-robin rotation through a list of addresses for failover support.
/// </summary>
public class AddressProvider
{
    private readonly List<string> _addresses;
    private int _currentIndex;

    /// <summary>
    /// Creates a new AddressProvider with the given list of addresses.
    /// </summary>
    /// <param name="addresses">List of addresses to rotate through</param>
    public AddressProvider(IReadOnlyList<string> addresses)
    {
        if (addresses.Count == 0)
        {
            throw new ArgumentException("At least one address must be provided", nameof(addresses));
        }

        _addresses = new List<string>(addresses);
        _currentIndex = 0;
    }

    /// <summary>
    /// Gets the current address without changing the index.
    /// </summary>
    public string CurrentAddress => _addresses[_currentIndex];

    /// <summary>
    /// Gets the host from the current address.
    /// </summary>
    public string CurrentHost => ParseHost(_addresses[_currentIndex]);

    /// <summary>
    /// Gets the port from the current address.
    /// </summary>
    public int CurrentPort => ParsePort(_addresses[_currentIndex]);

    /// <summary>
    /// Gets the number of addresses.
    /// </summary>
    public int AddressCount => _addresses.Count;

    /// <summary>
    /// Checks if there are multiple addresses available.
    /// </summary>
    public bool HasMultipleAddresses => _addresses.Count > 1;

    /// <summary>
    /// Rotates to the next address in round-robin fashion.
    /// </summary>
    /// <returns>The next address</returns>
    public string RotateToNextAddress()
    {
        _currentIndex = (_currentIndex + 1) % _addresses.Count;
        return CurrentAddress;
    }

    /// <summary>
    /// Parses the host from an address string (host:port format).
    /// </summary>
    private static string ParseHost(string address)
    {
        if (string.IsNullOrEmpty(address))
            return address;

        var index = address.LastIndexOf(':');
        if (index > 0)
        {
            return address.Substring(0, index);
        }

        return address;
    }

    /// <summary>
    /// Parses the port from an address string (host:port format).
    /// Returns -1 if no port is specified.
    /// </summary>
    private static int ParsePort(string address)
    {
        if (string.IsNullOrEmpty(address))
            return -1;

        var index = address.LastIndexOf(':');
        if (index >= 0 && index < address.Length - 1)
        {
            if (int.TryParse(address.Substring(index + 1), out var port))
            {
                return port;
            }
        }

        return -1;
    }
}
