using QuestDB.Ingress;

namespace net_questdb_client_tests;

public class TestSender : LineSender
{
    public HttpRequestMessage Request { get; set; }
    public HttpResponseMessage Response { get; set; }
    
    
    public TestSender(QuestDBOptions options) : base(options)
    {
    }

    public TestSender(string confString) : base(confString)
    {
    }
    
    public async Task<TestSender> SendAsync()
    {
        (Request, Response) = await base.SendAsync();
        return this;
    }
}