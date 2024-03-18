using System.Data.Common;
using System.Data.SqlClient;

namespace QuestDB;

public record ConfStr
{
    public string Service { get; set; }
    public DbConnectionStringBuilder Params { get; set; }


    public ConfStr(string confString)
    {
        // Expect the first part of the string to be a connection protocol
        Service = parseService(confString);
        // Thereafter its a normal connection string
        Params = new DbConnectionStringBuilder();
        Params.ConnectionString = confString[(Service.Length + 2)..];
    }


    private string parseService(string confString)
    {
        switch (confString[0])
        {
            case 't':
                // revisit
                return confString[0..2];
            case 'h':
                return confString[0..4];
            default:
                throw new NotImplementedException();
        }
    }
}