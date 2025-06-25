namespace QuestDB.Enums;

public enum DataType : byte
{
    DOUBLE = 10,
}

public static class DataTypeExtensions
{
    public static int byteSize(DataType dt)
    {
        switch (dt)
        {
            case DataType.DOUBLE:
                return sizeof(double);
            default:
                throw new NotImplementedException();
        }
    }
}