using Microsoft.Extensions.Options;
using Org.BouncyCastle.Tls;
using QuestDB.Ingress.Enums;
using QuestDB.Ingress.Senders;
using QuestDB.Ingress.Utils;
// ReSharper disable MemberCanBePrivate.Global

namespace QuestDB.Ingress;

/// <summary>
///     A factory for creating new instances of <see cref="ISender"/>
/// </summary>
/// <remarks>
///     For sole initialisation via config string, please use <see cref="New"/>. This does not require a call to <see cref="ISender.Build"/>.
///     <para />
///     If you wish set initial options via config string, and then modify others, please use <see cref="Configure(QuestDB.Ingress.Utils.QuestDBOptions?)"/>,
///     followed by record syntax, followed by <see cref="ISender.Build"/>.
///     <para />
///     If you wish to configure entirely programmatically, please use <see cref="Configure(QuestDB.Ingress.Utils.QuestDBOptions?)"/>,
///     and configure the options directly. Ensure that <see cref="ISender.Build"/> is called after configuration is complete.
/// </remarks>
public static class Sender
{
    /// <summary>
    ///     Creates and initialises a new instance of <see cref="ISender"/> from a configuration string.
    /// </summary>
    /// <param name="confStr"></param>
    /// <returns>An intialised <see cref="ISender"/></returns>
    public static ISender New(string confStr)
    {
        return Configure(confStr).Build();
    }
    
    /// <summary>
    ///     Performs initial configuration of <see cref="ISender"/>.
    ///     Must be followed by <see cref="ISender.Build"/> prior to use.
    /// </summary>
    /// <param name="options"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    public static ISender New(QuestDBOptions? options = null)
    {
        switch (options.protocol)
        {
            case ProtocolType.http:
            case ProtocolType.https:
                return new HttpSender(options);
            case ProtocolType.tcp:
            case ProtocolType.tcps:
                return new TcpSender(options);
            default:
                throw new NotImplementedException();
        }
    }
    
    /// <inheritdoc cref="Configure(QuestDB.Ingress.Utils.QuestDBOptions?)"/>
    public static QuestDBOptions Configure(string confStr)
    {
        return new QuestDBOptions(confStr);
    }
}