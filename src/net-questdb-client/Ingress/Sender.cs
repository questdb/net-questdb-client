using QuestDB.Ingress.Enums;
using QuestDB.Ingress.Senders;
using QuestDB.Ingress.Utils;

namespace QuestDB.Ingress;

public static class Sender
{
    public static ISender New(string confStr)
    {
        return Configure(confStr).Build();
    }

    public static ISender Configure(string confStr)
    {
        var options = new QuestDBOptions(confStr);

        switch (options.protocol)
        {
            case ProtocolType.http:
            case ProtocolType.https:
                return new HttpSender().Configure(options);
            case ProtocolType.tcp:
            case ProtocolType.tcps:
                return new TcpSender().Configure(options);
            default:

                throw new NotImplementedException();
        }
    }
}