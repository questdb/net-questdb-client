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


using NUnit.Framework;
using QuestDB.Ingress;

namespace net_questdb_client_tests;

public class QuestDBOptionsTests
{
    [Test]
    public void BasicParse()
    {
        Assert.That(
            new QuestDBOptions("http::addr=localhost:9000;").addr,
            Is.EqualTo("localhost:9000"));
    }

    [Test]
    public void CapitalCaseInValues()
    {
        Assert.That(
            new QuestDBOptions("http::aDdR=locALhOSt:9000;").addr,
            Is.EqualTo("locALhOSt:9000"));
    }

    [Test]
    public void CaseSensitivityForSchema()
    {
        Assert.That(
            () => new QuestDBOptions("hTTp::aDdR=locALhOSt:9000;"),
            Throws.TypeOf<IngressError>()
        );
    }

    [Test]
    public void DuplicateKey()
    {
        // duplicate keys are 'last writer wins'
        Assert.That(
            new QuestDBOptions("http::addr=localhost:9000;addr=localhost:9009;").addr,
            Is.EqualTo("localhost:9009"));
    }

    [Test]
    public void KeyCannotStartWithNumber()
    {
        // invalid property
        Assert.That(
            () => new QuestDBOptions("https::123=456;"),
            Throws.TypeOf<IngressError>().With.Message.Contains("Invalid property")
        );
    }

    [Test]
    public void DefaultConfig()
    {
        Assert.That(
            new QuestDBOptions("http::addr=localhost:9000;").ToString()
            , Is.EqualTo("http::addr=localhost:9000;auth_timeout=15000;auto_flush=on;auto_flush_bytes=2147483647;auto_flush_interval=1000;auto_flush_rows=75000;init_buf_size=65536;max_buf_size=104857600;max_name_len=127;request_min_throughput=102400;request_timeout=10000;retry_timeout=10000;tls_verify=on"));
    }

    [Test]
    public void InvalidProperty()
    {
        Assert.That(
            () => new QuestDBOptions("http::asdada=localhost:9000;"),
            Throws.TypeOf<IngressError>()
                .With.Message.Contains("Invalid property")
        );
    }

    [Test]
    public void RequireTrailingSemicolon()
    {
        Assert.That(
            () => new QuestDBOptions("http::addr=localhost:9000"),
            Throws.TypeOf<IngressError>().With.Message.Contains("semicolon")
        );
    }


    [Test]
    public void ParsingHostAndPort()
    {
        var options = new QuestDBOptions("http::addr=localhost:1234;");
        Assert.That(options.Port == 1234);
        Assert.That(options.Host == "localhost");
    }

    [Test]
    public void ParsingDefaultPorts()
    {
        var httpOptions = new QuestDBOptions("http::addr=localhost;");
        Assert.That(httpOptions.Port == 9000);

        var tcpOptions = new QuestDBOptions("tcp::addr=localhost;");
        Assert.That(tcpOptions.Port == 9009);
    }
}