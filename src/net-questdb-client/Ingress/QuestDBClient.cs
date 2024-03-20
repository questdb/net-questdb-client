namespace QuestDB.Ingress;

public class QuestDBClient
{
    private QuestDBOptions _config;
    private HttpClient _client;


    public QuestDBClient(IHttpClientFactory factory)
    {
        _client = factory.CreateClient();
    }

    public QuestDBClient(HttpClient client)
    {
        _client = client;
    }
}