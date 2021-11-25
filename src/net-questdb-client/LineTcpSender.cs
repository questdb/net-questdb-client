using System;
using System.Globalization;
using System.Net.Sockets;
using System.Text;

namespace QuestDB
{
    public class LineTcpSender : IDisposable
    {
        private static readonly long EpochTicks = new DateTime(1970, 1, 1).Ticks;
        private readonly TcpClient _client;
        private readonly bool _closeClient;
        private readonly byte[] _sendBuffer;
        private int _position;
        private bool _hasMetric;
        private bool _quoted;
        private bool _noFields = true;

        public LineTcpSender(TcpClient client, int bufferSize = 4096)
        {
            _client = client;
            _sendBuffer = new byte[bufferSize];
        }

        public LineTcpSender(String address, int port, int bufferSize = 4096) : this(new TcpClient(address, port), bufferSize)
        {
            _closeClient = true;
        }

        public LineTcpSender Metric(ReadOnlySpan<char> name)
        {
            if (_hasMetric)
            {
                throw new InvalidOperationException("duplicate metric");
            }

            _quoted = false;
            _hasMetric = true;
            EncodeUtf8(name);
            return this;
        }
        
        public LineTcpSender Tag(ReadOnlySpan<char> tag, ReadOnlySpan<char> value) {
            if (_hasMetric && _noFields) {
                Put(',').EncodeUtf8(tag).Put('=').EncodeUtf8(value);
                return this;
            }
            throw new InvalidOperationException("metric expected");
        }
        
        private LineTcpSender Field(ReadOnlySpan<char> name) {
            if (_hasMetric) {
                if (_noFields) {
                    Put(' ');
                    _noFields = false;
                } else {
                    Put(',');
                }

                return EncodeUtf8(name).Put('=');
            }
            throw new InvalidOperationException("metric expected");
        }
        
        public LineTcpSender Field(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
        {
            Field(name).Put('\"');
            _quoted = true;
            EncodeUtf8(value);
            _quoted = false;
            Put('\"');
            return this;
        }
        
        public LineTcpSender Field(ReadOnlySpan<char> name, long value) {
            Field(name).Put(value).Put('i');
            return this;
        }
        
        public LineTcpSender Field(ReadOnlySpan<char> name, double value) {
            Field(name).Put(value.ToString(CultureInfo.InvariantCulture));
            return this;
        }

        private LineTcpSender Put(long value)
        {
            if (value == long.MinValue)
            {
                // Special case, long.MinValue cannot be handled by QuestDB
                throw new ArgumentOutOfRangeException();
            }
            
            Span<byte> num = stackalloc byte[20];
            int pos = num.Length;
            long remaining = Math.Abs(value);
            do
            {
                long digit = remaining % 10;
                num[--pos] = (byte) ('0' + digit);
                remaining /= 10;
            } while (remaining != 0);
            
            if (value < 0)
            {
                num[--pos] = (byte) '-';
            }
            
            int len = num.Length - pos;
            if (_position + len >= _sendBuffer.Length)
            {
                Flush();
            }
            num.Slice(pos, len).CopyTo(_sendBuffer.AsSpan(_position));
            _position += len;

            return this;
        }

        private LineTcpSender EncodeUtf8(ReadOnlySpan<char> name)
        {
            for (int i = 0; i < name.Length; i++)
            {
                var c = name[i];
                if (c < 128)
                {
                    PutSpecial(c);
                }
                else
                {
                    PutUtf8(c);
                }
            }

            return this;
        }

        private void PutUtf8(char c)
        {
            if (_position + 4 >= _sendBuffer.Length)
            {
                Flush();
            }

            Span<byte> bytes = _sendBuffer;
            Span<char> chars = stackalloc char[1] {c};
            _position += Encoding.UTF8.GetBytes(chars, bytes.Slice(_position, 4));
        }

        private void PutSpecial(char c)
        {
            switch (c)
            {
                case ' ':
                case ',':
                case '=':
                    if (!_quoted)
                    {
                        Put('\\');
                    }
                    goto default;
                default:
                    Put(c);
                    break;
                case '\n':
                case '\r':
                    Put('\\').Put(c);
                    break;
                case '"':
                    if (_quoted)
                    {
                        Put('\\');
                    }

                    Put(c);
                    break;
                case '\\':
                    Put('\\').Put('\\');
                    break;
            }
        }

        private LineTcpSender Put(ReadOnlySpan<char> chars)
        {
            foreach (var c in chars)
            {
                Put(c);
            }

            return this;
        }
        
        private LineTcpSender Put(char c)
        {
            if (_position + 1 >= _sendBuffer.Length)
            {
                Flush();
            }

            _sendBuffer[_position++] = (byte) c;
            return this;
        }

        public void Flush()
        {
            _position -= _client.Client.Send(_sendBuffer, 0, _position, SocketFlags.None);
        }

        public void Dispose()
        {
            try
            {
                if (_position > 0)
                {
                    Flush();
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Error on disposing LineTcpClient: {0}", ex);
            }
            finally
            {
                if (_closeClient)
                {
                    _client.Dispose();
                }
            }
        }

        public void AtNow()
        {
            Put('\n');
            _hasMetric = false;
            _noFields = true;
        }

        public void At(DateTime timestamp)
        {
            long epoch = timestamp.Ticks - EpochTicks;
            Put(' ').Put(epoch).Put("00").AtNow();
        }
    }
}