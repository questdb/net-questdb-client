using QuestDB.Ingress;

namespace net_questdb_client_tests;

public class TestSender : LineSender
{
    public HttpRequestMessage? request { get; set; }
    public HttpResponseMessage? response { get; set; }
    
    public TestSender(string confString) : base(confString)
    {
    }
    
    public async Task<TestSender> SendAsync()
    {
        (request, response) = await base.SendAsync();
        return this;
    }
}