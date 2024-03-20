using System.Collections;
using System.Diagnostics;
using System.Globalization;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using Org.BouncyCastle.Asn1.Sec;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.Utilities.Encoders;

namespace QuestDB.Ingress;

public class LineSender
{
    private QuestDBOptions _options;
    private ChunkedBuffer _buffer;
    private Stopwatch _intervalTimer;
    
    public LineSender(QuestDBOptions options)
    {

    }

    public LineSender(string confString) : this(new QuestDBOptions(confString))
    {

    }
    
    public LineSender Table(ReadOnlySpan<char> name)
    {
        _buffer.Table(name);
        return this;
    }

    public LineSender Symbol(ReadOnlySpan<char> symbolName, ReadOnlySpan<char> value)
    {
        _buffer.Symbol(symbolName, value);
        return this;
    }

    public LineSender Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        _buffer.Column(name, value);
        return this;
    }

    public LineSender Column(ReadOnlySpan<char> name, long value)
    {
        _buffer.Column(name, value);
        return this;
    }
    
    public LineSender Column(ReadOnlySpan<char> name, bool value)
    {
        _buffer.Column(name, value);
        return this;
    }
    
    public LineSender Column(ReadOnlySpan<char> name, double value)
    {
        _buffer.Column(name, value);
        return this;
    }
    
    public LineSender Column(ReadOnlySpan<char> name, DateTime value)
    {
        _buffer.Column(name, value);
        return this;
    }
    
    public LineSender Column(ReadOnlySpan<char> name, DateTimeOffset value)
    {
        _buffer.Column(name, value);
        return this;
    }

    public LineSender At(DateTime timestamp)
    {
        _buffer.At(timestamp);
        HandleAutoFlush();
        return this;
    }

    private void HandleAutoFlush()
    {
        if (_options.AutoFlush == QuestDBOptions.AutoFlushType.on)
        {
            if (_buffer.RowCount >= _options.AutoFlushRows
                || (_intervalTimer.Elapsed >= _options.AutoFlushInterval))
            {
                Flush();
            }
        }
    }


    public LineSender At(DateTimeOffset timestamp)
    {
        _buffer.At(timestamp);
        HandleAutoFlush();
        return this;
    }

    private void Send()
    {
        
    }

    private async Task SendAsync()
    {
        
    }
    
    public LineSender Flush()
    {
        _buffer.Clear();
        return this;
    }

}
