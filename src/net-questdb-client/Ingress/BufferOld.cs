// using System.Text;
//
// namespace QuestDB.Ingress;
//
// public class BufferOld : List<byte>
// {
//     public int maxNameLen { get; }
//     
//     public int initCapacity { get; }
//     
//     private string? currentTable { get; set; }
//     
//     public BufferOld(int initCapacity = 65536, int maxNameLen = 127) : base(capacity: initCapacity)
//     {
//         this.maxNameLen = maxNameLen;
//         this.initCapacity = initCapacity;
//     }
//
//     /// <summary>
//     /// Ensure the buffer has at least `additional` bytes more capacity.
//     /// </summary>
//     /// <param name="additional"></param>
//     public void Reserve(int additional)
//     {
//         this.EnsureCapacity(this.Capacity + additional);
//     }
//
//     public void CheckIfTableIsSet()
//     {
//         if (currentTable is not null)
//         {
//             throw new IngressError(ErrorCode.InvalidApiCall, "Table already specified.");
//         }
//     }
//
//     public BufferOld Table(ReadOnlySpan<char> name)
//     {
//         CheckIfTableIsSet();
//         name.ValidateTableName();
//         
//     }
//     
//     
//     private void PutUtf8(char c)
//     {
//         if (_position + 4 >= _sendBuffer.Length) NextBuffer();
//
//         var bytes = _sendBuffer.AsSpan(_position);
//         Span<char> chars = stackalloc char[1] { c };
//         _position += Encoding.UTF8.GetBytes(chars, bytes);
//     }
//     
//     private void PutSpecial(char c)
//     {
//         switch (c)
//         {
//             case ' ':
//             case ',':
//             case '=':
//                 if (!_quoted) Put('\\');
//                 goto default;
//             default:
//                 Put(c);
//                 break;
//             case '\n':
//             case '\r':
//                 Put('\\').Put(c);
//                 break;
//             case '"':
//                 if (_quoted) Put('\\');
//
//                 Put(c);
//                 break;
//             case '\\':
//                 Put('\\').Put('\\');
//                 break;
//         }
//     }
//
//     // public void Symbol(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
//     // {
//     //   //  this.Append(Encoding.UTF8.GetBytes(name))
//     // }
//     
// }