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
    /// <summary>
    /// Creates an AddressProvider that manages round‑robin rotation over the provided addresses.
    /// </summary>
    /// <param name="addresses">A non-empty list of address strings (e.g., host or host:port) to rotate through.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="addresses"/> contains no elements.</exception>
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
    /// Gets or sets the current address index.
    /// Used internally to save/restore state during operations like /settings probing.
    /// </summary>
    internal int CurrentIndex
    {
        get => _currentIndex;
        set => _currentIndex = value;
    }

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
    /// <summary>
    /// Advances the current address index to the next address in round-robin order.
    /// </summary>
    public void RotateToNextAddress()
    {
        _currentIndex = (_currentIndex + 1) % _addresses.Count;
    }

    /// <summary>
    /// Parses the host from an address string.
    /// Supports both regular (host:port) and IPv6 ([ipv6]:port) formats.
    /// For IPv6 addresses, returns the complete bracketed form including '[' and ']'.
    /// <summary>
    /// Extracts the host portion from an address string that may include a port or a bracketed IPv6 literal.
    /// </summary>
    /// <param name="address">The address in forms like "host:port", "[ipv6]:port", or a bare host; may be null or empty.</param>
    /// <returns>The host portion: returns the bracketed "[ipv6]" when present, the substring before the last ':' for "host:port" forms, or the original input if no host separator is found or the input is null/empty.</returns>
    public static string ParseHost(string address)
    {
        if (string.IsNullOrEmpty(address))
            return address;

        // Handle IPv6 addresses in bracket notation: [ipv6]:port
        if (address.StartsWith("["))
        {
            var closingBracketIndex = address.IndexOf(']');
            if (closingBracketIndex > 0)
            {
                // Return the entire bracketed section as the host
                return address.Substring(0, closingBracketIndex + 1);
            }
        }

        // For non-bracketed addresses, use the last colon to split host and port
        var colonIndex = address.LastIndexOf(':');
        if (colonIndex > 0)
        {
            return address.Substring(0, colonIndex);
        }

        return address;
    }

    /// <summary>
    /// Parses the port from an address string.
    /// Supports both regular (host:port) and IPv6 ([ipv6]:port) formats.
    /// Returns -1 if no port is specified.
    /// <summary>
    /// Extracts the numeric port from an address string.
    /// </summary>
    /// <param name="address">An address in forms like "host:port", "[ipv6]:port", "host", or null/empty; the port portion, if present, is parsed as an integer.</param>
    /// <returns>`-1` if no valid port is present or parsing fails; otherwise the parsed port number.</returns>
    public static int ParsePort(string address)
    {
        if (string.IsNullOrEmpty(address))
            return -1;

        // Handle IPv6 addresses in bracket notation: [ipv6]:port
        if (address.StartsWith("["))
        {
            var closingBracketIndex = address.IndexOf(']');
            if (closingBracketIndex > 0 && closingBracketIndex < address.Length - 1)
            {
                // Check if there's a colon after the closing bracket
                if (address[closingBracketIndex + 1] == ':')
                {
                    var portString = address.Substring(closingBracketIndex + 2);
                    if (int.TryParse(portString, out var port))
                    {
                        return port;
                    }
                }
            }
            return -1;
        }

        // For non-bracketed addresses, use the last colon to split host and port
        var colonIndex = address.LastIndexOf(':');
        if (colonIndex >= 0 && colonIndex < address.Length - 1)
        {
            if (int.TryParse(address.Substring(colonIndex + 1), out var port))
            {
                return port;
            }
        }

        return -1;
    }
}