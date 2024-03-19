namespace QuestDB.Ingress;

public class IngressError : Exception
{
    public ErrorCode Code { get; }
    
    public IngressError(ErrorCode code, string? message) 
        : base(message)
    {
        this.Code = code;
    }
    
    public IngressError(ErrorCode code, string? message, Exception inner) 
        : base(message, inner)
    {
        this.Code = code;
    }

    public static IngressError ConfigSettingIsNull(string propertyName)
    {
        return new IngressError(ErrorCode.ConfigError, $"Property: {propertyName} is null.");
    }
}