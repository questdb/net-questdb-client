namespace QuestDB.Ingress;

public class IngressError : Exception
{
    public ErrorCode Code { get; }
    
    public IngressError(ErrorCode code, string? message) 
        : base($"{code.ToString()} : {message}")
    {
        this.Code = code;
    }
    
    public IngressError(ErrorCode code, string? message, Exception inner) 
        : base($"{code.ToString()} : {message}", inner)
    {
        this.Code = code;
    }

    public static IngressError ConfigSettingIsNull(string propertyName)
    {
        return new IngressError(ErrorCode.ConfigError, $"Property: {propertyName} is null.");
    }
}