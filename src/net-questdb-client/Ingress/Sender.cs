// using System.Runtime.CompilerServices;
//
// namespace QuestDB.Ingress;
//
// public class Sender
// {
//     private BufferOld _bufferOld;
//     
//     public Sender(string confString)
//     {
//         
//     }
//
//     public static Sender fromConf(string confString)
//     {
//         return new Sender(confString: confString);
//     }
//
//
//     public Sender Row(string trades,
//         IReadOnlyDictionary<string, string>? symbols,
//         IReadOnlyDictionary<string, object>? columns)
//     {
//         return this;
//     }
//
//     public Sender Flush()
//     {
//         return this;
//     }
// }