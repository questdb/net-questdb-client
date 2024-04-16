namespace QuestDB.Ingress.Utils;

public record JsonErrorResponse
{
    public string code { get; init; }
    public string message { get; init; }
    public int line { get; init; }
    public string errorId { get; init; }
    
    public override string ToString()
    {
        return $"\nServer Response (\n\tCode: `{code}`\n\tMessage: `{message}`\n\tLine: `{line}`\n\tErrorId: `{errorId}` \n)";
    }
}